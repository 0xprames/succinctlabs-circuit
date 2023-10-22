use crate::node::{GateOp, Node, NodeType, NodeValue};
use futures::future::{self, BoxFuture};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{
    broadcast::Sender,
    RwLock, // this is more expensive to use than std:sync::RwLock, but the code is unfortunately strucutured in a way
            // that makes it difficult to scope guard/drop a rlock before an await
};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Builder {
    // we use an Arc here instead of Box to pass around (traverse etc) a shared pointer to the inputs and their children, which constitute our computation graph
    // the lock here is necessary to provide interior mutability (e.g when setting a given node's->next pointer)
    inputs: Vec<Arc<RwLock<Node>>>,
    // val_map is used to "memoize" computation results for a given node - such that duplicate nodes that exist in the computation graph will not be recomputed
    // lock overhead necessary here bc the same node potentially can be evaluated/written at multiple points in the fill
    val_map: Arc<RwLock<HashMap<Uuid, u64>>>,
    // waiter map to broadcast value update notifs to evaluate() tasks that are waiting on a parent node to be evaluated
    // we don't wrap this with a lock bc the broadcast channel is instantiated only once when a node is created
    // which is not a "concurrent" write
    waiter_map: HashMap<Uuid, Sender<u64>>,
    // constraints holds a list of equality constraints between a pair of nodes
    // represented here by their node-id (uuid)
    constraints: Vec<(Uuid, Uuid)>,
}

impl Builder {
    pub fn new() -> Self {
        let val_map = Arc::from(RwLock::from(HashMap::new()));
        let waiter_map = HashMap::new();
        Builder {
            inputs: Vec::new(),
            val_map,
            waiter_map,
            constraints: Vec::new(),
        }
    }

    // Initializes a node in the graph
    pub async fn init(&mut self) -> Arc<RwLock<Node>> {
        let ret = Arc::from(RwLock::new(Node::input()));
        self.inputs.push(ret.clone());
        // TODO: it could eventually make sense to add a broadcaster here - but since we assume the ordering is:
        // set_inputs->fill_nodes->which starts at the input nodes' children, we don't need this for now
        ret
    }

    // Initializes a node in a graph, set to a constant value
    pub async fn constant(&mut self, value: u64) -> Node {
        let ret = Node::constant(value);
        let mut lock = self.val_map.write().await;
        lock.insert(ret.id, value);
        ret
    }

    // Adds 2 nodes in the graph, returning a new node
    pub async fn add(&mut self, a: Arc<RwLock<Node>>, b: Arc<RwLock<Node>>) -> Arc<RwLock<Node>> {
        let a_rlock = a.read().await;
        let b_rlock = b.read().await;
        let op = GateOp::Add(a_rlock.id, b_rlock.id);
        let ret = Arc::from(RwLock::from(Node::new(
            a_rlock.clone(),
            b_rlock.clone(),
            op,
        )));
        let (val_broadcaster, _) = tokio::sync::broadcast::channel(3);
        self.waiter_map.insert(ret.read().await.id, val_broadcaster);
        // note: there are some cases a and b are the same node, so take one write lock at a time to prev a deadlock
        drop(a_rlock);
        drop(b_rlock);
        let mut a_wlock = a.write().await;
        // skip setting constant nodes' next ptrs
        if a_wlock.node_type != NodeType::Constant {
            a_wlock.set_next(ret.clone()).await;
        }
        drop(a_wlock);
        let mut b_wlock = b.write().await;
        if b_wlock.node_type != NodeType::Constant {
            b_wlock.set_next(ret.clone()).await;
        }
        ret
    }

    // Multiplies 2 nodes in the graph, returning a new node
    pub async fn mul(&mut self, a: Arc<RwLock<Node>>, b: Arc<RwLock<Node>>) -> Arc<RwLock<Node>> {
        let a_rlock = a.read().await;
        let b_rlock = b.read().await;
        let op = GateOp::Mul(a_rlock.id, b_rlock.id);
        let ret = Arc::from(RwLock::from(Node::new(
            a_rlock.clone(),
            b_rlock.clone(),
            op,
        )));
        let (val_broadcaster, _) = tokio::sync::broadcast::channel(3);
        self.waiter_map.insert(ret.read().await.id, val_broadcaster);
        // note: there are some cases a and b are the same node, so take one write lock at a time to prev a deadlock
        drop(a_rlock);
        drop(b_rlock);
        // skip setting constant nodes' next ptr
        let mut a_wlock = a.write().await;
        if a_wlock.node_type != NodeType::Constant {
            a_wlock.set_next(ret.clone()).await;
        }
        drop(a_wlock);
        let mut b_wlock = b.write().await;
        if b_wlock.node_type != NodeType::Constant {
            b_wlock.set_next(ret.clone()).await;
        }
        drop(b_wlock);
        ret
    }

    // Asserts that 2 nodes are equal
    pub async fn assert_equal(&mut self, a: Arc<RwLock<Node>>, b: Arc<RwLock<Node>>) {
        self.constraints
            .push((a.read().await.id, b.read().await.id))
    }

    pub async fn set_input(&mut self, input: Arc<RwLock<Node>>, val: u64) {
        let input_inner: Node;
        {
            input_inner = input.read().await.to_owned();
        }
        // make sure node is an input - can't use iterator's sync closure w/ tokio rwlock await here so just classic for loop it
        let mut contains = false;
        for stored in &self.inputs {
            if stored.read().await.to_owned() == input_inner {
                contains = true;
            }
        }
        if input_inner.node_type != NodeType::Input || !contains {
            panic!("Node {} is not a valid input", input_inner)
        }
        let mut lock = self.val_map.write().await;
        lock.insert(input_inner.id, val);
        input.write().await.value = NodeValue::Static(Some(val));
    }

    pub async fn fill_nodes(&mut self) {
        // start the fill with the graph's inputs
        // every node in the computation graph is constructed downstream from (at least) one input node
        let mut task_handles = vec![];
        for input in self.inputs.clone() {
            println!("Filling from input: {}", input.read().await.to_owned());
            let mut captured_self = self.clone();
            task_handles.push(tokio::spawn(async move {
                captured_self
                    .evaluate(input.read().await.next.clone())
                    .await;
            }));
        }
        future::join_all(task_handles).await;
    }

    fn evaluate(&mut self, children: Vec<Arc<RwLock<Node>>>) -> BoxFuture<()> {
        async move {
            for node in children {
                // self is behind a (mut) shared ref - thus the clone/capture of these attributes into the tokio task closure
                let captured_val_map = self.val_map.clone();
                let captured_waiter_map = self.waiter_map.clone();
                let captured_node: Node;
                // scope guard for the node rlock
                {
                    captured_node = node.read().await.clone();
                }
                let mut captured_self = self.clone();
                tokio::spawn(async move {
                    // take a read lock, and check first to see if this node has already been evaluated elsewhere
                    let val_rlock = captured_val_map.read().await;
                    if let Some(_) = val_rlock.get(&captured_node.id) {
                        return;
                    }
                    match &captured_node.value {
                        NodeValue::Gate(op) => {
                            match op {
                                GateOp::Add(a, b) => {
                                    let maybe_a_val = val_rlock.get(&a);
                                    let maybe_b_val = val_rlock.get(&b);
                                    // check each "parent" to see if they've been evaluated by this point
                                    let a_val = match maybe_a_val {
                                        Some(a_val) => *a_val,
                                        None => {
                                            // setup waiter here for a to get evaluated
                                            if let Some(a_broadcaster) = captured_waiter_map.get(&a)
                                            {
                                                let mut rx = a_broadcaster.subscribe();

                                                println!(
                                                    "Waiting on parent {} to evaluate node: {}",
                                                    a, captured_node
                                                );
                                                rx.recv().await.unwrap()
                                            } else {
                                                // this should never occur
                                                panic!("broadcaster for node {} not found", a)
                                            }
                                        }
                                    };

                                    let b_val = match maybe_b_val {
                                        Some(b_val) => *b_val,
                                        None => {
                                            // setup waiter here for b to get evaluated
                                            if let Some(b_broadcaster) = captured_waiter_map.get(&b)
                                            {
                                                let mut rx = b_broadcaster.subscribe();
                                                println!(
                                                    "Waiting on parent {} to evaluate node: {}",
                                                    b, captured_node
                                                );
                                                rx.recv().await.unwrap()
                                            } else {
                                                // this should never occur
                                                panic!("broadcaster for node {} not found", b)
                                            }
                                        }
                                    };
                                    drop(val_rlock);
                                    println!(
                                        "Recieved parent values {} and {} for node: {}",
                                        a_val, b_val, captured_node
                                    );
                                    let node_val = a_val + b_val;
                                    // obtain a write lock for this node
                                    println!(
                                        "Evaluated add {} and storing value for node: {}",
                                        node_val, captured_node
                                    );
                                    // obtain a write lock for this node
                                    let mut wlock = captured_val_map.write().await;
                                    wlock.insert(captured_node.id, node_val);
                                    // send value update notif to any blocked evaluation paths
                                    if let Some(val_broadcaster) =
                                        captured_waiter_map.get(&captured_node.id)
                                    {
                                        match val_broadcaster.send(node_val) {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }
                                    }
                                    node.write().await.value = NodeValue::Static(Some(node_val));
                                }
                                GateOp::Mul(a, b) => {
                                    let maybe_a_val = val_rlock.get(&a);
                                    let maybe_b_val = val_rlock.get(&b);
                                    // check each "parent" to see if they've been evaluated by this point
                                    let a_val = match maybe_a_val {
                                        Some(a_val) => *a_val,
                                        None => {
                                            // setup waiter here for a to get evaluated
                                            if let Some(a_broadcaster) = captured_waiter_map.get(&a)
                                            {
                                                println!(
                                                    "Waiting on parent {} to evaluate node: {}\n",
                                                    a, captured_node
                                                );
                                                let mut rx = a_broadcaster.subscribe();
                                                rx.recv().await.unwrap()
                                            } else {
                                                // this should never occur
                                                panic!("broadcaster for node {} not found", a)
                                            }
                                        }
                                    };

                                    let b_val = match maybe_b_val {
                                        Some(b_val) => *b_val,
                                        None => {
                                            // setup waiter here for b to get evaluated
                                            if let Some(b_broadcaster) = captured_waiter_map.get(&b)
                                            {
                                                println!(
                                                    "Waiting on parent {} to evaluate node: {}\n",
                                                    b, captured_node
                                                );
                                                let mut rx = b_broadcaster.subscribe();
                                                rx.recv().await.unwrap()
                                            } else {
                                                // this should never occur
                                                panic!("broadcaster for node {} not found", b)
                                            }
                                        }
                                    };
                                    drop(val_rlock);
                                    println!(
                                        "Recieved parent values {} and {} for node: {}",
                                        a_val, b_val, captured_node
                                    );
                                    let node_val = a_val * b_val;
                                    println!(
                                        "Evaluated mul {} and storing value for node: {}",
                                        node_val, captured_node
                                    );
                                    // obtain a write lock for this node
                                    let mut wlock = captured_val_map.write().await;
                                    wlock.insert(captured_node.id, node_val);
                                    // send value update notif to any blocked evaluation paths
                                    if let Some(val_broadcaster) =
                                        captured_waiter_map.get(&captured_node.id)
                                    {
                                        match val_broadcaster.send(node_val) {
                                            Ok(_) => {}
                                            Err(_) => {}
                                        }
                                    }
                                    node.write().await.value = NodeValue::Static(Some(node_val));
                                }
                            }
                        }
                        NodeValue::Static(_) => {
                            // this is unexpected, because a static val means this node is either an input or a const
                            unreachable!()
                        }
                    }
                    captured_self.evaluate(captured_node.next).await;
                })
                .await
                .unwrap();
            }
        }
        .boxed()
    }

    // Given a graph that has `fill_nodes` already called on it
    // checks that all the constraints hold
    pub async fn check_constraints(&mut self) -> bool {
        let rlock = self.val_map.read().await;
        let mut ret = false;
        for (node_a, node_b) in &self.constraints {
            ret = rlock.get(node_a) == rlock.get(node_b)
        }
        ret
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // f(x) = x^2 + x + 5
    #[tokio::test]
    async fn test_prompt_base_case() {
        let mut builder = Builder::new();
        let x = builder.init().await;
        let x_squared = builder.mul(x.clone(), x.clone()).await;
        let five = builder.constant(5).await;
        let x_squared_plus_5 = builder
            .add(x_squared.clone(), Arc::from(RwLock::from(five)))
            .await;
        let res = builder.add(x_squared_plus_5, x.clone()).await;
        builder.set_input(x, 5).await;
        let expected = builder.constant(35).await;
        builder
            .assert_equal(res, Arc::from(RwLock::from(expected)))
            .await;
        builder.fill_nodes().await;
        assert_eq!(builder.check_constraints().await, true);
    }

    // f(x) = (x^2 + x) * x
    #[tokio::test]
    async fn test_third_degree_poly() {
        let mut builder = Builder::new();
        let x = builder.init().await;
        let x_squared = builder.mul(x.clone(), x.clone()).await;
        let x_squared_plus_x = builder.add(x_squared.clone(), x.clone()).await;
        let res = builder.mul(x_squared_plus_x, x.clone()).await;
        builder.set_input(x, 10).await;
        let expected = builder.constant(1100).await;
        builder
            .assert_equal(res, Arc::from(RwLock::from(expected)))
            .await;
        builder.fill_nodes().await;
        assert_eq!(builder.check_constraints().await, true);
    }

    // f(x,y) = (x^2 + x + 5) * (y^2 + y + 5)
    #[tokio::test]
    async fn test_multi_inputs() {
        let mut builder = Builder::new();
        let x = builder.init().await;
        let x_squared = builder.mul(x.clone(), x.clone()).await;
        let five = builder.constant(5).await;
        let x_squared_plus_5 = builder
            .add(x_squared.clone(), Arc::from(RwLock::from(five.clone())))
            .await;
        let x_squared_plus_x_plus_five = builder.add(x_squared_plus_5, x.clone()).await;
        let y = builder.init().await;
        let y_squared = builder.mul(y.clone(), y.clone()).await;
        let y_squared_plus_5 = builder
            .add(y_squared.clone(), Arc::from(RwLock::from(five)))
            .await;
        let y_squared_plus_y_plus_5 = builder.add(y_squared_plus_5, y.clone()).await;
        let res = builder
            .mul(x_squared_plus_x_plus_five, y_squared_plus_y_plus_5)
            .await;
        builder.set_input(x, 5).await;
        builder.set_input(y, 6).await;
        let expected = builder.constant(1645).await;
        builder
            .assert_equal(res, Arc::from(RwLock::from(expected)))
            .await;
        builder.fill_nodes().await;
        assert_eq!(builder.check_constraints().await, true);
    }

    // f(x) = (x^2 + x + 5)
    // f(y) = (y^2 + y + 5)
    // assert f(x) - f(y) = 0 or that f(x) == f(y)
    #[tokio::test]
    async fn test_equality_constraints() {
        let mut builder = Builder::new();
        let x = builder.init().await;
        let x_squared = builder.mul(x.clone(), x.clone()).await;
        let five = builder.constant(5).await;
        let x_squared_plus_5 = builder
            .add(x_squared.clone(), Arc::from(RwLock::from(five.clone())))
            .await;
        let x_squared_plus_x_plus_five = builder.add(x_squared_plus_5, x.clone()).await;
        let y = builder.init().await;
        let y_squared = builder.mul(y.clone(), y.clone()).await;
        let y_squared_plus_5 = builder
            .add(y_squared.clone(), Arc::from(RwLock::from(five)))
            .await;
        let y_squared_plus_y_plus_5 = builder.add(y_squared_plus_5, y.clone()).await;
        builder
            .assert_equal(x_squared_plus_x_plus_five, y_squared_plus_y_plus_5)
            .await;
        builder.set_input(x, 5).await;
        builder.set_input(y, 5).await;
        builder.fill_nodes().await;
        assert_eq!(builder.check_constraints().await, true);
    }

    // f(x) = (x^2 + x + 5) -> x = 5
    // f(y) = (y^2 + y + 5) -> y = 6
    // assert f(x) - f(y) != 0 or that f(x) != f(y)
    #[tokio::test]
    async fn test_equality_constraints_fails() {
        let mut builder = Builder::new();
        let x = builder.init().await;
        let x_squared = builder.mul(x.clone(), x.clone()).await;
        let five = builder.constant(5).await;
        let x_squared_plus_5 = builder
            .add(x_squared.clone(), Arc::from(RwLock::from(five.clone())))
            .await;
        let x_squared_plus_x_plus_five = builder.add(x_squared_plus_5, x.clone()).await;
        let y = builder.init().await;
        let y_squared = builder.mul(y.clone(), y.clone()).await;
        let y_squared_plus_5 = builder
            .add(y_squared.clone(), Arc::from(RwLock::from(five)))
            .await;
        let y_squared_plus_y_plus_5 = builder.add(y_squared_plus_5, y.clone()).await;
        builder
            .assert_equal(x_squared_plus_x_plus_five, y_squared_plus_y_plus_5)
            .await;
        builder.set_input(x, 5).await;
        builder.set_input(y, 6).await;
        builder.fill_nodes().await;
        assert_eq!(builder.check_constraints().await, false);
    }
}
