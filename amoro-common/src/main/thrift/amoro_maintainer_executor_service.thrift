namespace java org.apache.amoro.api

include "amoro_commons.thrift"

struct ExecutorTask {
    1: ExecutorTaskId taskId;
    2: optional binary taskInput;
    3: optional map<string, string> serverConfig;
}

struct ExecutorTaskId {
    1: i64 processId;
    2: i32 taskId;
}

struct ExecutorTaskResult {
    1: ExecutorTaskId taskId;
    2: string catalog;
    3: string database;
    4: string table;
    5: string tableType;
    6: i32 status;
    7: i32 threadId;
    8: optional binary taskOutput;
    9: optional string errorMessage;
    10: optional map<string, string> summary;
}

service MaintainerService {

  void ping()

  ExecutorTask ackTableMetadata(
                1: string catalog,
                2: string db,
                3: string tableName
                4: string type)
  throws(1: amoro_commons.AmoroException e1)

  void completeTask(
                1: ExecutorTaskResult taskResult)
  throws (1: amoro_commons.AmoroException e1)

}