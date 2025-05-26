namespace java org.apache.amoro.api

include "amoro_commons.thrift"

struct ExecutorTask {
    1: ExecutorTaskId taskId;
    2: optional binary taskInput;
    3: optional map<string, string> properties;
}

struct ExecutorTaskId {
    1: i64 processId;
    2: i32 taskId;
}

struct ExecutorTaskResult {
    1: ExecutorTaskId taskId;
    2: i32 threadId;
    3: optional binary taskOutput;
    4: optional string errorMessage;
    5: optional map<string, string> summary;
}

service MaintainerService {

  void ping()

  ExecutorTask ackTableMetadata(1: string catalog,
               2: string db,
                3: string tableName
                4: string type)
            throws(1: amoro_commons.AmoroException e1)

  void completeTask(1: string type,
                    2: ExecutorTaskResult taskResult)
              throws (1: amoro_commons.AmoroException e1)

}