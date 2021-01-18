
package org.apache.dolphinscheduler.dao.entity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import java.util.List;


public class Task {

    /**
     * task node type
     */
    private String type;

    /**
     * task node id
     */
    private String id;

    /**
     * task node name
     */
    private String name;

    /**
     * params information
     */
    @JsonDeserialize(using = JSONUtils.JsonDataDeserializer.class)
    @JsonSerialize(using = JSONUtils.JsonDataSerializer.class)
    private String params;

    /**
     * task node description
     */
    private String description;


    /**
     * the run flag has two states, NORMAL or FORBIDDEN
     */
    private String runFlag = "NORMAL";


    @JsonDeserialize(using = JSONUtils.JsonDataDeserializer.class)
    @JsonSerialize(using = JSONUtils.JsonDataSerializer.class)
    private String conditionResult;

    /**
     * outer dependency information
     */
    @JsonDeserialize(using = JSONUtils.JsonDataDeserializer.class)
    @JsonSerialize(using = JSONUtils.JsonDataSerializer.class)
    private String dependence;

    /**
     * maximum number of retries
     */
    private int maxRetryTimes;

    /**
     * Unit of retry interval: points
     */
    private int retryInterval;

    /**
     * delay execution time.
     */
    private int delayTime;

    /**
     * task time out
     */
    @JsonDeserialize(using = JSONUtils.JsonDataDeserializer.class)
    @JsonSerialize(using = JSONUtils.JsonDataSerializer.class)
    private String timeout;

    @JsonDeserialize(using = JSONUtils.JsonDataDeserializer.class)
    @JsonSerialize(using = JSONUtils.JsonDataSerializer.class)
    private String waitStartTimeout;

    /**
     * task instance priority
     */
    private Priority taskInstancePriority;

    /**
     * worker group
     */
    private String workerGroup;


    private List preTasks;


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRunFlag() {
        return runFlag;
    }

    public void setRunFlag(String runFlag) {
        this.runFlag = runFlag;
    }

    public String getConditionResult() {
        return conditionResult;
    }

    public void setConditionResult(String conditionResult) {
        this.conditionResult = conditionResult;
    }

    public String getDependence() {
        return dependence;
    }

    public void setDependence(String dependence) {
        this.dependence = dependence;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public int getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }

    public String getWaitStartTimeout() {
        return waitStartTimeout;
    }

    public void setWaitStartTimeout(String waitStartTimeout) {
        this.waitStartTimeout = waitStartTimeout;
    }

    public Priority getTaskInstancePriority() {
        return taskInstancePriority;
    }

    public void setTaskInstancePriority(Priority taskInstancePriority) {
        this.taskInstancePriority = taskInstancePriority;
    }

    public String getWorkerGroup() {
        return workerGroup;
    }

    public void setWorkerGroup(String workerGroup) {
        this.workerGroup = workerGroup;
    }

    public List getPreTasks() {
        return preTasks;
    }

    public void setPreTasks(List preTasks) {
        this.preTasks = preTasks;
    }


    public static final class TaskBuilder {
        private String type;
        private String id;
        private String name;
        private String params;
        private String description;
        private String runFlag = "NORMAL";
        private String conditionResult;
        private String dependence;
        private int maxRetryTimes;
        private int retryInterval;
        private int delayTime;
        private String timeout;
        private String waitStartTimeout;
        private Priority taskInstancePriority;
        private String workerGroup;
        private List preTasks;

        private TaskBuilder() {
        }

        public static TaskBuilder aTask() {
            return new TaskBuilder();
        }

        public TaskBuilder withType(String type) {
            this.type = type;
            return this;
        }

        public TaskBuilder withId(String id) {
            this.id = id;
            return this;
        }

        public TaskBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public TaskBuilder withParams(String params) {
            this.params = params;
            return this;
        }

        public TaskBuilder withDescription(String description) {
            this.description = description;
            return this;
        }

        public TaskBuilder withRunFlag(String runFlag) {
            this.runFlag = runFlag;
            return this;
        }

        public TaskBuilder withConditionResult(String conditionResult) {
            this.conditionResult = conditionResult;
            return this;
        }

        public TaskBuilder withDependence(String dependence) {
            this.dependence = dependence;
            return this;
        }

        public TaskBuilder withMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public TaskBuilder withRetryInterval(int retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        public TaskBuilder withDelayTime(int delayTime) {
            this.delayTime = delayTime;
            return this;
        }

        public TaskBuilder withTimeout(String timeout) {
            this.timeout = timeout;
            return this;
        }

        public TaskBuilder withWaitStartTimeout(String waitStartTimeout) {
            this.waitStartTimeout = waitStartTimeout;
            return this;
        }

        public TaskBuilder withTaskInstancePriority(Priority taskInstancePriority) {
            this.taskInstancePriority = taskInstancePriority;
            return this;
        }

        public TaskBuilder withWorkerGroup(String workerGroup) {
            this.workerGroup = workerGroup;
            return this;
        }

        public TaskBuilder withPreTasks(List preTasks) {
            this.preTasks = preTasks;
            return this;
        }

        public Task build() {
            Task task = new Task();
            task.setType(type);
            task.setId(id);
            task.setName(name);
            task.setParams(params);
            task.setDescription(description);
            task.setRunFlag(runFlag);
            task.setConditionResult(conditionResult);
            task.setDependence(dependence);
            task.setMaxRetryTimes(maxRetryTimes);
            task.setRetryInterval(retryInterval);
            task.setDelayTime(delayTime);
            task.setTimeout(timeout);
            task.setWaitStartTimeout(waitStartTimeout);
            task.setTaskInstancePriority(taskInstancePriority);
            task.setWorkerGroup(workerGroup);
            task.setPreTasks(preTasks);
            return task;
        }
    }
}
