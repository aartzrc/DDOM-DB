using Lambda;
using Reflect;
using Type;
using StringTools;

/**
 * This is the root level repository of data, it provides some basic lookups and events
 */
@:allow(DDOM, DDOMInst, DDOMSelectorProcessor)
class DDOMStatic {
    // Lookup maps, for speed mostly - this could be handled with one large Array
    static var dataByType:Map<String, Array<DataNode>> = [];
    static var dataById:Map<String, DataNode> = [];

    static var listeners:Map<String, Array<(event:String, ddom:DDOM)->Void>> = [];

    /**
     * Add a listener for the specified event name (or * for all events), return is a function that can be used for detaching the callback - or use 'off'
     * @param event 
     * @param callback 
     * @return ()->Void
     */
    public static function on(event:String, callback:(event:String, ddom:DDOM)->Void) {
        if(!listeners.exists(event)) listeners[event] = [];
        var cbs = listeners[event];
        if(cbs.indexOf(callback) == -1)
            cbs.push(callback);
        return off.bind(event, callback);
    }

    /**
     * Remove a listener, or ALL listeners for an event by passing a null callback function
     * @param event 
     * @param callback 
     * @return ->Void)
     */
    public static function off(event:String, callback:(event:String, ddom:DDOM)->Void) {
        if(!listeners.exists(event)) return false;
        if(callback == null && event != "*") return listeners.remove(event);
        var cbs = listeners[event];
        return cbs.remove(callback);
    }

    static function fire(event:String, ddom:DDOM) {
        if(listeners.exists("*"))
            for(cb in listeners["*"])
                cb(event, ddom);
        if(!listeners.exists(event)) return;
        for(cb in listeners[event])
            cb(event, ddom);
    }

	static function create(type:String):DDOM {
        var dn = new DataNode(type);
        if(!dataByType.exists(type)) dataByType[type] = [];
        dataByType[type].push(dn);
        // A bit of trickery to maintain ctor consistency - empty selector returns an empty nodes result, then we populate with the single item that was just created
        var ddom = new DDOMInst("");
        ddom.nodes.push(dn);
        fire("create", ddom);
        return ddom;
    }

    static function getById(id:String):DDOM {
        return new DDOMInst("#" + id);
    }

    static function getByType(type:String):DDOM {
        return new DDOMInst(type);
    }

    /**
     * Select from the root data set
     * @param selector 
     * @return DDOM
     */
    static function select(selector:String):DDOM {
        return new DDOMInst(selector);
    }
}

@:allow(DDOM, DDOMIterator, DDOMSelectorProcessor, DDOMStatic)
class DDOMInst {
    var nodes:Array<DataNode>; // If parent and nodes are null, the selector will pull from the root data set
    var selector:{selector:String, parent:DDOM};
    function new(selector:String = "*", parent:DDOM = null) {
        this.selector = {selector:selector,parent:parent};
        // Special case, ignore empty string selector for use when we do not want to populate the nodes directly
        if(selector != "") 
            this.nodes = DDOMSelectorProcessor.process(selector, parent);
        else
            this.nodes = [];
    }

    // TODO: on/off per DDOMInst - any way to consolidate the selectors?
    // maybe each 'sub' call can trace up the stack to the root 'select' so they can stay isolated, then cache these at DDOMStatic. a selector can't change once it is applied to a DDOMInst, so this should be solid

    /**
     * Returns all unique children of the nodes available in this DDOM
     * @return DDOM
     */
    public function children():DDOM {
        return new DDOMInst("* > *", this); // Get all direct children of nodes in this DDOM
    }

    public function parents():DDOM {
        return new DDOMInst("* < *", this); // Get all parents of nodes in this DDOM
    }

    /**
     * Append all nodes within the provided DDOM to all nodes within this DDOM
     * @param child 
     */
    public function append(child:DDOM) {
        var coreChild:DDOMInst = cast child;
        // Verify the children are part of the data set
        if(coreChild.nodes.exists((n) -> DDOMStatic.dataByType[n.type].indexOf(n) == -1)) throw "Detached DDOM, unable to appendChild";
        for(node in nodes) {
            for(cn in coreChild.nodes) {
                if(node.children.indexOf(cn) == -1) {
                    cn.parents.push(node);
                    node.children.push(cn);
                    // TODO: fire events
                }
            }
        }
    }

    /**
     * Remove/detach all the children in the provided DDOM from all nodes in this DDOM - does NOT delete the child
     * @param child 
     */
    public function remove(child:DDOM) {
        var coreChild:DDOMInst = cast child;
        for(node in nodes) {
            for(cn in coreChild.nodes) {
                trace(cn);
                node.children.remove(cn);
                // TODO: fire events
            }
        }
    }

    /**
     * Detach from all parents and remove from the lookup tables - this becomes a detached DDOM and cannot be used again!
     */
    public inline function delete() {
        for(pn in parents()) pn.remove(this);
        for(node in nodes) {
            var id = node.fields.field("id");
            if(id != null) DDOMStatic.dataById.remove(id);
            DDOMStatic.dataByType[node.type].remove(node);
            DDOMStatic.fire("delete", this);
        }
    }

    public inline function size() {
        return nodes.length;
    }

    function fieldWrite<T>(name:String, value:T) {
        if(nodes.length == 0) return;
        var node = nodes[0];
        // Lock down `id` value, must be a String and non-duplicate in the current data set
        if(name == "id") {
            if(!Std.is(value, String)) throw "`DDOM.id` must be a `String`";
            var newId:String = cast value;
            var prevId:String = cast nodes.fields.field("id");
            if(newId != prevId) {
                if(DDOMStatic.dataById.exists(newId)) throw "Unable to set `DDOM.id`, duplicate id value found";
                if(prevId != null) DDOMStatic.dataById.remove(prevId);
                DDOMStatic.dataById.set(newId, node);
            }
        }
        // Verify type remains the same
        var f = node.fields.field(name);
        if(f != null && !Std.is(value, Type.getClass(f))) throw "Data type must remain the same for field `" + name + "` : " + f + " (" + Type.getClass(f).getClassName() + ") != " + value + " (" + Type.getClass(value).getClassName() + ")";
        node.fields.setField(name, value);
        // TODO: fire events
    }

    function fieldRead<T>(name:String):T {
        if(nodes.length == 0) return null;
        return nodes[0].fields.field(name);
    }

    function arrayRead(n:Int):DDOM {
        return new DDOMInst("*:eq(" + n + ")"); // Get all, then choose the 'n'th item
    }

    public function iterator():Iterator<DDOM> {
        return new DDOMIterator(nodes);
    }

    public function toString() {
        return Std.string(nodes);
    }

    /**
     * Select a sub-set of this DDOM
     * @param selector 
     * @return DDOM
     */
    public function sub(selector:String):DDOM {
        return new DDOMInst(selector, this);
    }
}

@:allow(DDOMInst)
class DDOMIterator {
    var i:Int = 0;
    var nodes:Array<DataNode>;
    function new(nodes:Array<DataNode>) {
        this.nodes = nodes;
    }
    public function hasNext() {
        return i < nodes.length;
    }
    public function next() {
        var ddom = new DDOMInst("");
        ddom.nodes.push(nodes[i++]);
        return ddom;
    }
}

@:forward(iterator, append, children, size, delete, remove, sub)
abstract DDOM(DDOMInst) from DDOMInst to DDOMInst {
    @:op(a.b)
    public function fieldWrite<T>(name:String, value:T) this.fieldWrite(name, value);
    @:op(a.b)
    public function fieldRead<T>(name:String):T return this.fieldRead(name);
    @:op([]) 
    public function arrayRead(n:Int) return this.arrayRead(n);

    public static function create(type:String) return DDOMStatic.create(type);
    public static function getById(id:String) return DDOMStatic.getById(id);
    public static function getByType(type:String) return DDOMStatic.getByType(type);
    public static function select(selector:String) return DDOMStatic.select(selector);
}

// This is the actual data item, DDOM wraps this
@:allow(DDOMInst, DDOMSelectorProcessor, DDOMStatic)
class DataNode {
    var type:String;
    var fields = {};
    var children:Array<DataNode> = [];
    var parents:Array<DataNode> = [];
    
	function new(type:String) {
        this.type = type;
    }

    public function toString() {
        var id = fields.field("id");
        return "{type:" + type + (id != null ? ",id:" + id : "") + "}";
    }
}

@:allow(DDOMInst)
class DDOMSelectorProcessor {
    /* Notes:
        selectors groups are comma separated, white space is required as a token separator
        selectors can be chained (not sure how yet...)
        # id
        < parent - eg: "user < *" will get all parents of the user type, "user < session" will get the sessions of all users - css uses ! token, but it's wacky so I decided on < instead
        > direct child - eg: "user > session" will get sessions of all users, "user! > cart! > product[name=paper]" will get users with a cart that have products with name "paper"
        * all - eg: "*[name=paper]" will get any type with a name "paper"
        ' ' (space) all descendents - eg: "user product" will get all products for all users
        ~ get siblings - eg: "user ~ employee" will get all employees that are data-siblings of users
        :eq(x) get at position - eg: "cart > product:eq(0)" get the first product in the cart

        TODO: store the selector within the DDOM and make DDOM 'observable', when a data update occurs re-run the selector and notify any listeners
    */
    static function process(selector:String, parent:DDOMInst = null):Array<DataNode> {
        if(parent == null) { // null parent means use all data
            parent = new DDOMInst("");
            parent.nodes = DDOMStatic.dataByType.flatten();
        }

        var results:Array<DataNode> = [];

        for(sel in selector.split(",")) { // Break into selector groups
            for(n in processTokens(sel.split(" "), parent.nodes)) // Break selector into tokens and process
                if(results.indexOf(n) == -1) results.push(n); // Merge results of all selector groups
        }

        return results;
    }

    static function processTokens(tokens:Array<String>, allNodes:Array<DataNode>):Array<DataNode> {
        var resultNodes:Array<DataNode> = [];
        var prevType = null;
        for(t in tokens) {
            t = t.trim();
            // Ignore empties
            if(t.length > 0) {
                // First char is the main token
                switch(t.charAt(0)) {
                    case "*": // all selector
                        resultNodes = [];
                        for(n in allNodes)
                            if(resultNodes.indexOf(n) == -1) resultNodes.push(n);
                        allNodes = resultNodes;
                        t = t.substr(1);
                    case "#": // id selector
                        resultNodes = [];
                        var id = t.substr(1);
                        var n = allNodes.find((n) -> n.fields.field("id") == id );
                        if(n != null && resultNodes.indexOf(n) == -1) resultNodes.push(n);
                        allNodes = resultNodes;
                        t = t.substr(id.length + 1);
                    case ">": // direct children selector
                        allNodes = [];
                        for(n in resultNodes) for(c in n.children) if(allNodes.indexOf(c) == -1) allNodes.push(c);
                        resultNodes = [];
                        t = t.substr(1);
                    case "<": // parent selector
                        allNodes = [];
                        for(n in resultNodes) for(p in n.parents) if(allNodes.indexOf(p) == -1) allNodes.push(p);
                        resultNodes = [];
                        t = t.substr(1);
                    case _: // Default to type selection
                        var type = t;
                        // Check for sub tokens
                        var st1 = t.indexOf("[");
                        var st2 = t.indexOf(":");
                        if(st1 > 0) {
                            type = type.substr(0, st1);
                        } else if(st2 > 0) {
                            type = type.substr(0, st2);
                        }
                        if(prevType != null) {
                            resultNodes = recurseChildrenByType(allNodes, type, [], []);
                        } else {
                            resultNodes = [];
                            for(n in allNodes.filter((n) -> n.type == type))
                                if(resultNodes.indexOf(n) == -1) resultNodes.push(n);
                            allNodes = resultNodes;
                        }
                        t = t.substr(type.length);

                        prevType = type;
                }
                // Check for 'sub' tokens
                if(t.length > 0)
                    resultNodes = processSubToken(t, resultNodes);
            }
        }
        return resultNodes;
    }

    static function recurseChildrenByType(allNodes:Array<DataNode>, type:String, found:Array<DataNode>, searched:Array<DataNode>):Array<DataNode> {
        for(n in allNodes) {
            if(searched.indexOf(n) == -1) {
                if(n.type == type)
                    if(found.indexOf(n) == -1) found.push(n);
                recurseChildrenByType(n.children, type, found, searched);
            }
        }
        return found;
    }

    static function processSubToken(subToken:String, nodes:Array<DataNode>) {
        switch(subToken.charAt(0)) {
            case ":": // query selector
                var q = subToken.substr(1);
                var m = [ "eq", "gt", "lt" ].find((t) -> q.indexOf(t) == 0);
                switch(m) {
                    case "eq":
                        q = q.substr(3);
                        var pos = Std.parseInt(q);
                        if(pos > nodes.length) nodes = [];
                            else nodes = [ nodes[pos] ];
                    case "gt":
                        q = q.substr(3);
                        var pos = Std.parseInt(q);
                        if(pos > nodes.length) nodes = [];
                            else nodes = [ for(i in pos ... nodes.length) nodes[i] ];
                    case "lt":
                        q = q.substr(3);
                        var pos = Std.parseInt(q) - 1;
                        if(pos > 0) {
                            if(pos > nodes.length) pos = nodes.length;
                            nodes = [ for(i in 0 ... pos) nodes[i] ];
                        }
                }
            case "[": // attribute/field selector
                trace("TODO: attribute selector");
        }
        return nodes;
    }
}