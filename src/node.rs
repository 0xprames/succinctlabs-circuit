use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum NodeType {
    Input,
    Constant,
    Dependent,
}
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum GateOp {
    Add(Uuid, Uuid),
    Mul(Uuid, Uuid),
}
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum NodeValue {
    Gate(GateOp),
    // TODO: this attribute needs to be modified after the memoized val_map is updated, to reflect in our Display trait for better printing
    Static(Option<u64>),
}
#[derive(Clone, Debug)]
pub struct Node {
    pub node_type: NodeType,
    pub value: NodeValue,
    pub next: Vec<Arc<RwLock<Node>>>,
    pub id: Uuid,
}

impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        // Note: we can't compare self.next or other.next because of the tokio::RwLock needing an await to read the inner data
        // this is suboptimal and partialy why the node_id attribute was introduced
        // tokio::RwLock (and std::RwLock) doesn't have a default PartialEq and the trait itself is non-async
        // and thus doesn't seem to allow for async/await syntax within its impl
        self.value == other.value && self.id == other.id && self.node_type == other.node_type
    }
}

impl Eq for Node {}

impl Node {
    pub fn input() -> Self {
        Node {
            node_type: NodeType::Input,
            value: NodeValue::Static(None),
            next: Vec::new(),
            id: Uuid::new_v4(),
        }
    }

    pub fn constant(val: u64) -> Self {
        Node {
            node_type: NodeType::Constant,
            value: NodeValue::Static(Some(val)),
            next: Vec::new(),
            id: Uuid::new_v4(),
        }
    }

    pub fn new(a: Node, b: Node, op: GateOp) -> Self {
        let ret = Node {
            node_type: NodeType::Dependent,
            value: NodeValue::Gate(op),
            next: Vec::new(),
            id: Uuid::new_v4(),
        };
        ret
    }

    pub async fn set_next(&mut self, next: Arc<RwLock<Node>>) {
        self.next.push(next)
    }
}
