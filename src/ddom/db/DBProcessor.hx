package ddom.db;

import haxe.Timer;
using Lambda;
using LambdaExt;

import ddom.DDOM;
import ddom.Selector;
import ddom.Processor;

import sys.db.Mysql;

/**
 * A typical database processor for an existing database, each table has a 'type' of data and fields. Parent-child relationships are provided via typeMaps.
 * Does not handle insert/update, only selecting is currently supported
 */
@:access(ddom.DDOMInst, ddom.DataNode)
class DBProcessor extends Processor implements IProcessor {
    var c:sys.db.Connection;
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
        c = Mysql.connect(params);
    }

    public function dispose() {
        if(c != null) c.close();
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
        var sqlLimit:String = null;
        var unhandledFilters:Array<TokenFilter> = [];
        for(filter in filters) {
            switch(filter) {
                case Pos(pos):
                    sqlLimit = 'LIMIT 1 OFFSET $pos';
                case OrderAsc(name):
                    sqlOrderBy = 'ORDER BY ${c.quote(name)} ASC';
                case OrderDesc(name):
                    sqlOrderBy = 'ORDER BY ${c.quote(name)} DESC';
                case ValEq(name, val):
                    sqlAnd.push("`" + name + "`='" + c.escape(val) + "'");
                case Id(id):
                    var curMap = typeMap[type];
                    if(curMap != null && curMap.idCol != null)
                        sqlAnd.push("`" + curMap.idCol + "`='" + c.escape(id) + "'");
                case StartsWith(name, val):
                    sqlAnd.push("`" + name + "` like '" + c.escape(val) + "%'");
                case Contains(name, val):
                    sqlAnd.push("`" + name + "` like '%" + c.escape(val) + "%'");
                case _:
                    unhandledFilters.push(filter);
            }
        }

        if(unhandledFilters.length > 0) trace(unhandledFilters);

        return {sql:sqlAnd, sqlOrderBy:sqlOrderBy, sqlLimit:sqlLimit, filters:unhandledFilters};
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
                var result = c.request(sql);
                for(row in result) results.push(toDataNode(t, row));
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
                        pcm[cm.type].push(c.quote(pn.fields["id"]));
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
                    } else {
                        sql = "select * from " + childType.table + " where " + childMap.parentIdCol + " in (" + pids.join(",") + ")";
                    }
                    log.push("query: " + sql);
                    //trace(sql);
                    for(row in c.request(sql))
                        childNodes.pushUnique(toDataNode(childType, row));
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
                    idMap[childMap].push(c.quote(cn.fields["id"]));
                }
            }
            for(childMap => ids in idMap) {
                var sql = "select * from " + parentTypeMap.table + " where " + parentTypeMap.idCol + " in (select " + childMap.parentIdCol + " from " + childMap.table + " where " + childMap.childIdCol + " in (" + ids.join(",") + "))";
                log.push("query: " + sql);
                //trace(sql);
                for(row in c.request(sql))
                    parentNodes.pushUnique(toDataNode(parentTypeMap, row));
            }
        }
        //trace("parents: " + (Timer.stamp() - t));

        return filters.length > 0 && parentNodes.length > 0 ? processFilter(parentNodes, filters) : parentNodes;
    }

    function toDataNode(t:TypeMap, row:Dynamic):DataNode {
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
        var fields = [ for(f in Reflect.fields(row)) f => Std.string(Reflect.field(row, f)) ];
        if(t.idCol != "id") fields.set("id", fields[t.idCol]); // Make sure 'id' field is available
        var dn = new DataNode(t.type, fields);
        return useCache ? checkCache(dn) : dn;
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
