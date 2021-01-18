package org.apache.dolphinscheduler.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.dolphinscheduler.dao.entity.DepTask;
import org.apache.dolphinscheduler.dao.entity.User;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public interface DepTaskService {
    Map<String, Object> createTask(User loginUser, String projectName, DepTask depTask) throws InvocationTargetException, IllegalAccessException, JsonProcessingException;

    Map<String, Object> releaseTask(User loginUser, String projectName, int taskId, int releaseState);

    Map<String, Object> queryTaskById(User loginUser, String projectName, Integer taskId);

    Map<String, Object> queryDepTaskListPaging(User loginUser, String projectName, String searchVal, Integer pageNo, Integer pageSize, Integer userId);

    Map<String, Object> queryDepTaskList(User loginUser, String projectName);

    Map<String, Object> deleteDepTaskById(User loginUser, String projectName, Integer processDefinitionId);

    Map<String, Object> queryTaskByName(User loginUser, String projectName, String taskNameName);
}
