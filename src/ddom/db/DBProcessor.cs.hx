package ddom.db;

import haxe.Timer;
using Lambda;
using LambdaExt;

import ddom.DDOM;
import ddom.Selector;
import ddom.Processor;

import mysqlconnector.*;

/**
 * A typical database processor for an existing database, each table has a 'type' of data and fields. Parent-child relationships are provided via typeMaps.
 * Does not handle insert/update, only selecting is currently supported
 */
@:access(ddom.DDOMInst, ddom.DataNode)
class DBProcessor extends Processor implements IProcessor {
    var c:mysqlconnector.MySqlConnection;

    var typeMap:Map<String, TypeMap> = [];
    var cache:Map<String, Map<String, DataNode>> = [];
    var useCache:Bool;
    var selectGroupCache:Map<String, Array<DataNode>> = [];

	/**
	 * Pass standard Haxe DB connection settings and TypeMap definitions per table, useCache defaults to true
	 * @param params 
	 * @param typeMaps 
	 * @param useCache 
	 */
	public function new(params : {
		host : String,
		?port : Int,
		user : String,
		pass : String,
		?socket : String,
		database : String
	}, typeMaps:Array<TypeMap> = null, useCache:Bool = true) {
        super([]);
        if(typeMaps != null) {
            for(t in typeMaps) {
                typeMap.set(t.type, t);
            }
        }
        this.useCache = useCache;

        var connectionString = 'server=${params.host};user=${params.user};password=${params.pass};database=${params.database}';
        c = new MySqlConnection(connectionString);
    }

    public function dispose() {
        c.DisposeAsync().GetAwaiter().GetResult();
    }

    public function select(selector:Selector = null):DDOM {
        return new DDOMInst(this, selector);
    }

    override function processGroup(group:SelectorGroup):Array<DataNode> {
        // Simple cache here to improve response speeds, it would be better to do this via query select/filter
        var sel:Selector = [group];
        if(!selectGroupCache.exists(sel))
            selectGroupCache.set(sel, super.processGroup(group));
        return selectGroupCache[sel];
    }

    function filtersToSql(type:String, filters:Array<TokenFilter>) {
        var sqlAnd:Array<String> = [];
        var sqlOrderBy:String = null;
        var sqlParams:Array<String> = [];
        var sqlLimit:String = null;
        var unhandledFilters:Array<TokenFilter> = [];
        for(filter in filters) {
            switch(filter) {
                case Pos(pos):
                    sqlLimit = 'LIMIT 1 OFFSET $pos';
                case OrderAsc(name):
                    sqlOrderBy = 'ORDER BY @p${sqlParams.length} ASC';
                    sqlParams.push(name);
                case OrderDesc(name):
                    sqlOrderBy = 'ORDER BY @p${sqlParams.length} DESC';
                    sqlParams.push(name);
                case ValEq(name, val):
                    sqlAnd.push("`" + name + "`=@p" + sqlParams.length);
                    sqlParams.push(val);
                case Id(id):
                    var curMap = typeMap[type];
                    if(curMap != null && curMap.idCol != null) {
                        sqlAnd.push("`" + curMap.idCol + "`=@p" + sqlParams.length);
                        sqlParams.push(id);
                    }
                case StartsWith(name, val):
                    sqlAnd.push("`" + name + "` like @p" + sqlParams.length);
                    sqlParams.push(val + "%");
                case Contains(name, val):
                    sqlAnd.push("`" + name + "` like @p" + sqlParams.length);
                    sqlParams.push("%" + val + "%");
                case _:
                    unhandledFilters.push(filter);
            }
        }

        if(unhandledFilters.length > 0) trace(unhandledFilters);

        return {sql:sqlAnd, sqlOrderBy:sqlOrderBy, sqlLimit:sqlLimit, sqlParams:sqlParams, filters:unhandledFilters};
    }

    override function selectOfType(type:String, filters:Array<TokenFilter>):Array<DataNode> {
        //var t = Timer.stamp();
        var results:Array<DataNode> = [];
        if(type == "." || type == "*") { // Get EVERYTHING - this should be optionally blocked?
            for(t in typeMap)
                results = results.concat(selectOfType(t.type, filters));
        } else {
            var t = typeMap[type];
            if(t == null) t = { type:type, table:type } // Nothing defined? try a default table
            var sql:String = null;
            try {
                sql = "select * from " + t.table;
                var sql2 = filtersToSql(type, filters);
                if(sql2.sql.length > 0)
                    sql = sql + " WHERE " + sql2.sql.join(" AND ");
                if(sql2.sqlOrderBy != null)
                    sql = sql + " " + sql2.sqlOrderBy;
                if(sql2.sqlLimit != null)
                    sql = sql + " " + sql2.sqlLimit;
                filters = sql2.filters;
                log.push("query: " + sql);
                //trace(sql);
                results = results.concat(buildResults(sql, sql2.sqlParams, t));
            } catch (e:Dynamic) {
#if debug
                trace(sql);
                trace(e);
#end
            }
        }
        //trace(Timer.stamp() - t);
        return filters.length > 0 && results.length > 0 ? processFilter(results, filters) : results;
    }

    override function selectChildren(parentNodes:Array<DataNode>, childType:String, filters:Array<TokenFilter>):Array<DataNode> {
        //var t = Timer.stamp();
        var childNodes:Array<DataNode> = [];
        var parentChildMap:Map<String, Map<String, Array<String>>> = [];
        for(pn in parentNodes) {
            var ptm = typeMap[pn.type];
            if(ptm != null && ptm.children != null) {
                var childMaps = (childType == "*" || childType == ".") ? ptm.children : ptm.children.filter((cm) -> cm.type == childType);
                if(childMaps.length > 0) {
                    if(!parentChildMap.exists(ptm.type)) parentChildMap.set(ptm.type, []);
                    var pcm = parentChildMap[ptm.type];
                    for(cm in childMaps) {
                        if(!pcm.exists(cm.type)) pcm.set(cm.type, []);
                        pcm[cm.type].push("'" + pn.fields["id"] + "'");
                    }
                }
            }
        }
        for(pt => cm in parentChildMap) {
            var parentType = typeMap[pt];
            for(ct => pids in cm) {
                if(pids.length > 0) {
                    var childType = typeMap[ct];
                    var childMap = parentType.children.find((cm) -> cm.type == ct);

                    var sql:String;
                    var params:Array<String>;
                    if(childMap.childIdCol != null) {
                        sql = "select * from " + childType.table + " where " + childType.idCol + " in (select " + childMap.childIdCol + " from " + childMap.table + " where " + childMap.parentIdCol + " in (" + pids.join(",") + "))";
                        var sql2 = filtersToSql(ct, filters);
                        if(sql2.sql.length > 0)
                            sql = sql + " AND " + sql2.sql.join(" AND ");
                        if(sql2.sqlOrderBy != null)
                            sql = sql + " " + sql2.sqlOrderBy;
                        if(sql2.sqlLimit != null)
                            sql = sql + " " + sql2.sqlLimit;
                        filters = sql2.filters;
                        params = sql2.sqlParams;
                    } else {
                        sql = "select * from " + childType.table + " where " + childMap.parentIdCol + " in (" + pids.join(",") + ")";
                        params = [];
                    }
                    log.push("query: " + sql);
                    //trace(sql);
                    for(r in buildResults(sql, params, childType))
                        childNodes.pushUnique(r);
                }
            }
        }
        //trace("children: " + (Timer.stamp() - t));
        return filters.length > 0 && childNodes.length > 0 ? processFilter(childNodes, filters) : childNodes;
    }

    override function selectParents(childNodes:Array<DataNode>, parentType:String, filters:Array<TokenFilter>):Array<DataNode> {
        //var t = Timer.stamp();
        var parentNodes:Array<DataNode> = [];
        var parentTypeMaps:Array<TypeMap> = [];
        if(parentType == "*" || parentType == ".") {
            for(cn in childNodes)
                parentTypeMaps = parentTypeMaps.concat(typeMap.filter((tm) -> tm.children != null && tm.children.exists((c) -> c.type == cn.type)).filter((pt) -> parentTypeMaps.indexOf(pt) == -1));
        } else {
            if(typeMap.exists(parentType)) parentTypeMaps = [typeMap[parentType]];
        }
        for(parentTypeMap in parentTypeMaps) {
            var idMap:Map<ChildTypeMap, Array<String>> = [];
            // Find all children that map back to this parent type and consolidate the lookups
            for(cn in childNodes) {
                var childMap = parentTypeMap.children.find((cm) -> cm.type == cn.type && cm.childIdCol != null);
                if(childMap != null) {
                    if(!idMap.exists(childMap)) idMap.set(childMap, []);
                    idMap[childMap].push("'" + cn.fields["id"] + "'");
                }
            }
            for(childMap => ids in idMap) {
                var sql = "select * from " + parentTypeMap.table + " where " + parentTypeMap.idCol + " in (select " + childMap.parentIdCol + " from " + childMap.table + " where " + childMap.childIdCol + " in (" + ids.join(",") + "))";
                log.push("query: " + sql);
                //trace(sql);
                for(r in buildResults(sql, [], parentTypeMap))
                    parentNodes.pushUnique(r);
            }
        }
        //trace("parents: " + (Timer.stamp() - t));

        return filters.length > 0 && parentNodes.length > 0 ? processFilter(parentNodes, filters) : parentNodes;
    }

    function toDataNode(t:TypeMap, reader:MySqlDataReader):DataNode {
        function checkCache(node:DataNode) {
            var type = node.type;
            var id = node.fields["id"];
            if(id == null) return node; // Cannot cache without id
            if(!cache.exists(type)) cache.set(type, new Map<String, DataNode>());
            if(!cache[type].exists(id)) {
                cache[type][id] = node;
                return node;
            }
            return cache[type][id];
        }
        var columns = reader.GetColumnSchema();
        var fields = [ for(i in 0 ... columns.Count) columns[i].ColumnName => Std.string(reader.GetValue(i)) ];
        if(t.idCol != "id") fields.set("id", fields[t.idCol]); // Make sure 'id' field is available
        var dn = new DataNode(t.type, fields);
        return useCache ? checkCache(dn) : dn;
    }

    function buildResults(sql:String, params:Array<String>, type:TypeMap) {
        var results:Array<DataNode> = [];
        try {
            c.Open();
            var cmd = new MySqlCommand(sql, c);
            for(i in 0 ... params.length)
                cmd.Parameters.AddWithValue('p$i', params[i]);
            var reader = cmd.ExecuteReader();
            while(reader.Read()) results.push(toDataNode(type, reader));
            reader.Dispose();
            cmd.Dispose();
            c.Close();
        } catch (e:Dynamic) {
#if debug
            trace(sql);
            trace(e);
            log.push(e);
#end
        }

        return results;
    }

    function quote(s:String) {
        return '\'$s\'';
    }
}

typedef TypeMap = {
    type:String,
    table:String,
    ?idCol:String,
    ?children:Array<ChildTypeMap>
}

typedef ChildTypeMap = {
    type:String,
    table:String,
    parentIdCol:String,
    ?childIdCol:String
}
