package ddom;

import ddom.DDOMSelectorProcessor;

using Lambda;
using Reflect;
using Type;

@:allow(ddom.DDOM, ddom.DDOMIterator, ddom.DDOMSelectorProcessor, ddom.DDOMStore)
class DDOMInst extends DDOMEventManager {
    var store:DDOMStore;
    var nodes:Array<DataNode>; // If parent and nodes are null, the selector will pull from the root data set
    var selector:DDOMSelector;
    function new(store:DDOMStore, selector:DDOMSelector, parent:DDOMInst = null) {
        super();
        this.store = store;
        this.selector = selector; // Do not do selector chaining, multi-selectors/etc just get too complex - use the event system to chain updates
        // Special case, ignore empty selector for use when we do not want to populate the nodes directly
        if(selector.length > 0)
            this.nodes = DDOMSelectorProcessor.process(store, selector, parent);
        else
            this.nodes = [];
    }

    // TODO: on/off per DDOMInst - any way to consolidate the selectors?
    // TODO: auto-attach/detach from parent during on/off so child selectors can properly fire when parents change

    /**
     * Returns all unique children of the nodes available in this DDOM
     * @return DDOM
     */
    public function children():DDOM {
        return new DDOMInst(store, "* > *", this); // Get all direct children of nodes in this DDOM
    }

    public function parents():DDOM {
        return new DDOMInst(store, "* < *", this); // Get all parents of nodes in this DDOM
    }

    /**
     * Append all nodes within the provided DDOM to all nodes within this DDOM
     * @param child 
     */
    public function append(child:DDOM) {
        var coreChild:DDOMInst = cast child;
        // Verify the children are part of the data set
        if(coreChild.nodes.exists((n) -> store.dataByType[n.type].indexOf(n) == -1)) throw "Detached DDOM, unable to appendChild";
        var changed = false;
        for(node in nodes) {
            for(cn in coreChild.nodes) {
                if(node.children.indexOf(cn) == -1) {
                    cn.parents.push(node);
                    node.children.push(cn);
                    changed = true;
                }
            }
        }
        if(changed) fire(ResultChanged(this));
        return changed;
    }

    /**
     * Remove/detach all the children in the provided DDOM from all nodes in this DDOM - does NOT delete the child
     * @param child 
     */
    public function remove(child:DDOM) {
        var coreChild:DDOMInst = cast child;
        var changed = false;
        for(node in nodes) {
            for(cn in coreChild.nodes) {
                if(node.children.remove(cn)) changed = true;
            }
        }
        if(changed) fire(ResultChanged(this));
        return changed;
    }

    /**
     * Detach from all parents and remove from the lookup tables - this becomes a detached DDOM and cannot be used again!
     */
    public inline function delete() {
        for(pn in parents()) pn.remove(this);
        for(node in nodes) {
            var id = node.fields.field("id");
            if(id != null) store.dataById.remove(id);
            store.dataByType[node.type].remove(node);
            store.fire(Deleted(this));
        }
    }

    public inline function size() {
        return nodes.length;
    }

    /**
     * Updates all nodes in this DDOM with the name.value
     * @param name 
     * @param value 
     */
    function fieldWrite<T>(name:String, value:T) {
        if(nodes.length == 0) return;
        for(node in nodes) {
            // Lock down `id` value, must be a String
            if(name == "id") {
                if(!Std.is(value, String)) throw "`DDOM.id` must be a `String`";
                var newId:String = cast value;
                var prevId:String = cast nodes.fields.field("id");
                if(newId != prevId) {
                    if(prevId != null) store.dataById[prevId].remove(node);
                    if(!store.dataById.exists(newId)) store.dataById[newId] = [];
                    store.dataById[newId].push(node);
                }
            }
            // Verify type remains the same
            var f = node.fields.field(name);
            if(f != null && !Std.is(value, Type.getClass(f))) throw "Data type must remain the same for field `" + name + "` : " + f + " (" + Type.getClass(f).getClassName() + ") != " + value + " (" + Type.getClass(value).getClassName() + ")";
            node.fields.setField(name, value);
        }
        fire(FieldChanged(name, value));
    }

    /**
     * Gets first node and returns the field value (or null if no data available)
     * @param name 
     * @return T
     */
    function fieldRead<T>(name:String):T {
        if(nodes.length == 0) return null;
        return nodes[0].fields.field(name);
    }

    function arrayRead(n:Int):DDOM {
        return new DDOMInst(store, "*:eq(" + n + ")"); // Get all, then choose the 'n'th item
    }

    public function iterator():Iterator<DDOM> {
        return new DDOMIterator(store, nodes);
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
        return new DDOMInst(store, selector, this);
    }
}

@:allow(ddom.DDOMInst)
class DDOMIterator {
    var i:Int = 0;
    var nodes:Array<DataNode>;
    var store:DDOMStore;
    function new(store:DDOMStore, nodes:Array<DataNode>) {
        this.store = store;
        this.nodes = nodes;
    }
    public function hasNext() {
        return i < nodes.length;
    }
    public function next() {
        var ddom = new DDOMInst(store, "");
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
}

// This is the actual data item, DDOM wraps this
@:allow(ddom.DDOMInst, ddom.DDOMSelectorProcessor, ddom.DDOMStore)
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

enum DDOMEvent {
    Created(ddom:DDOM);
    Deleted(ddom:DDOM);
    ResultChanged(ddom:DDOM);
    FieldChanged(field:String, value:Any);
}