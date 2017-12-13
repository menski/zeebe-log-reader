

use failure::Error;
use rmp_serde::Deserializer;
use serde::Deserialize;
use serde_bytes::ByteBuf;
use std::collections::HashMap;
use std::fmt;

macro_rules! enum_serialize {
    ($e:ty => {$( $t:path => $s:expr ),* }) => {
impl<'d> ::serde::Deserialize<'d> for $e {

    fn deserialize<D>(deserializer: D) -> Result<$e, D::Error>
    where
        D: ::serde::Deserializer<'d>,
    {
        struct EnumVisitor;

        impl<'d> ::serde::de::Visitor<'d> for EnumVisitor {
            type Value = $e;

            fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                formatter.write_str("an string representation of enum")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
                where
                    E: ::serde::de::Error,
            {
                match value {
                    $($s => Ok($t),) *
                    _ => Err(E::custom(format!("Unsupported enum value: {}", value))),
                }
            }
        }

        deserializer.deserialize_str(EnumVisitor)
    }
}

};
}

#[derive(PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowEvent {
    pub bpmn_process_id: String,
    pub version: i32,
    pub bpmn_xml: ByteBuf,
    pub deployment_key: i64,
}

impl fmt::Debug for WorkflowEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("WorkflowEvent")
            .field("bpmn_process_id", &self.bpmn_process_id)
            .field("version", &self.version)
            .field("deployment_key", &self.deployment_key)
            .finish()
    }
}

#[derive(PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowInstanceEvent {
    pub state: WorkflowInstanceState,
    pub bpmn_process_id: String,
    pub version: i32,
    pub workflow_key: i64,
    pub workflow_instance_key: i64,
    pub activity_id: Option<String>,
    pub payload: ByteBuf,
}

impl fmt::Debug for WorkflowInstanceEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("WorkflowInstanceEvent")
            .field("state", &self.state)
            .field("bpmn_process_id", &self.bpmn_process_id)
            .field("version", &self.version)
            .field("workflow_key", &self.workflow_key)
            .field("workflow_instance_key", &self.workflow_instance_key)
            .field("activity_id", &self.activity_id)
            .finish()
    }
}

#[derive(Debug, PartialEq, Hash, Eq)]
pub enum WorkflowInstanceState {
    CreateWorkflowInstance,
    WorkflowInstanceCreated,
    WorkflowInstanceRejected,

    StartEventOccurred,
    EndEventOccurred,

    SequenceFlowTaken,

    GatewayActivated,

    ActivityReady,
    ActivityActivated,
    ActivityCompleting,
    ActivityCompleted,
    ActivityTerminated,

    WorkflowInstanceCompleted,

    CancelWorkflowInstance,
    WorkflowInstanceCanceled,
    CancelWorkflowInstanceRejected,

    UpdatePayload,
    PayloadUpdated,
    UpdatePayloadRejected,
}

enum_serialize!{
    WorkflowInstanceState => {
        WorkflowInstanceState::CreateWorkflowInstance => "CREATE_WORKFLOW_INSTANCE",
        WorkflowInstanceState::WorkflowInstanceCreated => "WORKFLOW_INSTANCE_CREATED",
        WorkflowInstanceState::WorkflowInstanceRejected => "WORKFLOW_INSTANCE_REJECTED",

        WorkflowInstanceState::StartEventOccurred => "START_EVENT_OCCURRED",
        WorkflowInstanceState::EndEventOccurred => "END_EVENT_OCCURRED",

        WorkflowInstanceState::SequenceFlowTaken => "SEQUENCE_FLOW_TAKEN",

        WorkflowInstanceState::GatewayActivated => "GATEWAY_ACTIVATED",

        WorkflowInstanceState::ActivityReady => "ACTIVITY_READY",
        WorkflowInstanceState::ActivityActivated => "ACTIVITY_ACTIVATED",
        WorkflowInstanceState::ActivityCompleting => "ACTIVITY_COMPLETING",
        WorkflowInstanceState::ActivityCompleted => "ACTIVITY_COMPLETED",
        WorkflowInstanceState::ActivityTerminated => "ACTIVITY_TERMINATED",

        WorkflowInstanceState::WorkflowInstanceCompleted => "WORKFLOW_INSTANCE_COMPLETED",

        WorkflowInstanceState::CancelWorkflowInstance => "CANCEL_WORKFLOW_INSTANCE",
        WorkflowInstanceState::WorkflowInstanceCanceled => "WORKFLOW_INSTANCE_CANCELED",
        WorkflowInstanceState::CancelWorkflowInstanceRejected => "CANCEL_WORKFLOW_INSTANCE_REJECTED",

        WorkflowInstanceState::UpdatePayload => "UPDATE_PAYLOAD",
        WorkflowInstanceState::PayloadUpdated => "PAYLOAD_UPDATED",
        WorkflowInstanceState::UpdatePayloadRejected => "UPDATE_PAYLOAD_REJECTED"
    }
}


#[derive(PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskEvent {
    pub state: TaskState,
    pub lock_time: i64,
    pub retries: i32,
    #[serde(rename = "type")]
    pub task_type: String,
    pub headers: TaskHeaders,
    pub custom_headers: HashMap<String, String>,
    pub payload: ByteBuf,
}

impl fmt::Debug for TaskEvent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TaskEvent")
            .field("state", &self.state)
            .field("lock_time", &self.lock_time)
            .field("retries", &self.retries)
            .field("type", &self.task_type)
            .field("headers", &self.headers)
            .field("custom_headers", &self.custom_headers)
            .finish()
    }
}

#[derive(Debug, PartialEq, Hash, Eq)]
pub enum TaskState {
    Create,
    Created,

    Lock,
    Locked,
    LockRejected,

    Complete,
    Completed,
    CompleteRejected,

    ExpireLock,
    LockExpired,
    LockExpirationRejected,

    Fail,
    Failed,
    FailRejected,

    UpdateRetries,
    RetriesUpdated,
    UpdateRetriesRejected,

    Cancel,
    Canceled,
    CancelRejected,
}

enum_serialize!{
    TaskState => {
        TaskState::Create => "CREATE",
        TaskState::Created => "CREATED",

        TaskState::Lock => "LOCK",
        TaskState::Locked => "LOCKED",
        TaskState::LockRejected => "LOCK_REJECTED",

        TaskState::Complete => "COMPLETE",
        TaskState::Completed => "COMPLETED",
        TaskState::CompleteRejected => "COMPLETE_REJECTED",

        TaskState::ExpireLock => "EXPIRE_LOCK",
        TaskState::LockExpired => "LOCK_EXPIRED",
        TaskState::LockExpirationRejected => "LOCK_EXPIRATION_REJECTED",

        TaskState::Fail => "FAIL",
        TaskState::Failed => "FAILED",
        TaskState::FailRejected => "FAIL_REJECTED",

        TaskState::UpdateRetries => "UPDATE_RETRIES",
        TaskState::RetriesUpdated => "RETRIES_UPDATED",
        TaskState::UpdateRetriesRejected => "UPDATE_RETRIES_REJECTED",

        TaskState::Cancel => "CANCEL",
        TaskState::Canceled => "CANCELED",
        TaskState::CancelRejected => "CANCEL_REJECTED"
    }
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskHeaders {
    workflow_instance_key: i64,
    bpmn_process_id: String,
    workflow_definition_version: i32,
    workflow_key: i64,
    activity_id: String,
    activity_instance_key: i64,
}

pub fn deserialize<'d, D: Deserialize<'d>>(data: &[u8]) -> Result<D, Error> {
    let mut de = Deserializer::new(data);
    let value = Deserialize::deserialize(&mut de)?;
    Ok(value)
}
