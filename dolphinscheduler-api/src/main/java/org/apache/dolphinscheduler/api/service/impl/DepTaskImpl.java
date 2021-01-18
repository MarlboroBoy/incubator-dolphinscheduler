package org.apache.dolphinscheduler.api.service.impl;/*
 * Copyright 2020, Zetyun DEP All rights reserved.
 */

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.BaseService;
import org.apache.dolphinscheduler.api.service.DepTaskService;
import org.apache.dolphinscheduler.api.service.ProcessDefinitionService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.dao.entity.DepProcessBuild;
import org.apache.dolphinscheduler.dao.entity.DepTask;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.Task;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.DepTaskMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DepTaskImpl extends BaseService implements DepTaskService {

    private static final String PROCESSDEFINITIONID = "processDefinitionId";
    private static final String DENPENDENCE = "DENPENDENCE";

    @Autowired
    private DepTaskMapper depTaskMapper;

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProjectService projectService;


    @Autowired
    private ProcessDefinitionService processDefinitionService;


    @Override
    @Transactional(isolation= Isolation.DEFAULT,propagation= Propagation.REQUIRED,rollbackFor = Exception.class)
    public Map<String, Object> createTask(User loginUser, String projectName, DepTask depTask)  throws InvocationTargetException, IllegalAccessException, JsonProcessingException {

        Map<String, Object> map = processDefinitionService.verifyProcessDefinitionName(loginUser, projectName,
                depTask.getName());
        if(((Status) map.get(Constants.STATUS)).getCode()!=0){
            return map;
        }
        Map<String, Object> result = new HashMap<>();
        Project project = projectMapper.queryByName(projectName);
        // check project auth
        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }
        List<Task> tasks = new ArrayList<>();
        Task taskBuild = Task.TaskBuilder.aTask()
                .withType(depTask.getType())
                .withId(depTask.getId())
                .withName(depTask.getName())
                .withParams(depTask.getParams())
                .withDescription(depTask.getDescription())
                .withRunFlag(depTask.getRunFlag())
                .withConditionResult(depTask.getConditionResult())
                .withDependence("{}")
                .withMaxRetryTimes(depTask.getMaxRetryTimes())
                .withRetryInterval(depTask.getRetryInterval())
                .withDelayTime(depTask.getDelayTime())
                .withTimeout(depTask.getTimeout())
                .withWaitStartTimeout("{}")
                .withTaskInstancePriority(depTask.getTaskInstancePriority())
                .withWorkerGroup(depTask.getWorkerGroup())
                .withPreTasks(new ArrayList()).build();
        DepProcessBuild processData = new DepProcessBuild();
        tasks.add(taskBuild);
        processData.setGlobalParams(new ArrayList<>());

        if(StringUtils.isNotEmpty(depTask.getDependence())){
            Map<String,Object> timeOut = new HashMap<>();
            timeOut.put("strategy","");
            timeOut.put("interval",null);
            timeOut.put("enable",false);
            Map<String,Object> waitStartTimeout = new HashMap<String, Object>(){{
                put("strategy","FAILED");
                put("interval",null);
                put("checkInterval",null);
                put("enable",false);
            }};
            Task denpenceTask = Task.TaskBuilder.aTask()
                    .withType("DEPENDENT")
                    .withId(depTask.getId()+1)
                    .withName(depTask.getName()+DENPENDENCE)
                    .withParams("{}")
                    .withDescription(depTask.getDescription())
                    .withRunFlag(depTask.getRunFlag())
                    .withConditionResult(depTask.getConditionResult())
                    .withDependence(depTask.getDependence())
                    .withMaxRetryTimes(0)
                    .withRetryInterval(1)
                    .withDelayTime(0)
                    .withTimeout(JSONUtils.toJsonString(timeOut))
                    .withWaitStartTimeout(JSONUtils.toJsonString(waitStartTimeout))
                    .withTaskInstancePriority(depTask.getTaskInstancePriority())
                    .withWorkerGroup(depTask.getWorkerGroup())
                    .withPreTasks(new ArrayList()).build();
            tasks.add(denpenceTask);
        }
        processData.setTasks(tasks);

        processData.setTenantId(depTask.getTenantId());
        String processDefinitionJson = JSONUtils.toJsonString(processData);
        Map<String, Object> processDefinition = processDefinitionService.createProcessDefinition(loginUser,
                projectName, depTask.getName(),
                processDefinitionJson, depTask.getDescription(), parseLocations(processData.getTasks()),
                parseConnects(processData.getTasks()));
        Status status = (Status) processDefinition.get(Constants.STATUS);
        if(status.getCode() == 0){
            Integer  id  = (Integer) processDefinition.get(PROCESSDEFINITIONID);
            depTask.setProcessId(id);
            depTask.setProjectId(project.getId());
            depTaskMapper.insertSchedulerTask(depTask);
            //result.put(Constants.DATA_LIST, processDefineMapper.selectById(processDefine.getId()));
            putMsg(result, Status.SUCCESS);
            result.put(PROCESSDEFINITIONID, depTask.getId());
            return result;
        }else {
            return processDefinition;
        }

    }

    @Override
    @Transactional(isolation= Isolation.DEFAULT,propagation= Propagation.REQUIRED,rollbackFor = Exception.class)
    public Map<String, Object> releaseTask(User loginUser, String projectName, int taskId, int releaseState) {
        Map<String,Object> result = new HashMap<>();
        DepTask depTask = depTaskMapper.selectDepTaskById(taskId);
        Map<String, Object> map = processDefinitionService.releaseProcessDefinition(loginUser, projectName,
                depTask.getProcessId(), releaseState);
        Status status = (Status) map.get(Constants.STATUS);
        if(status.getCode() == 0){
            depTask.setReleaseState(releaseState);
            depTaskMapper.updateDepTask(depTask);
            //result.put(Constants.DATA_LIST, processDefineMapper.selectById(processDefine.getId()));
            putMsg(result, Status.SUCCESS);
            return result;
        }else {
            return map;
        }
    }

    @Override
    public Map<String, Object> queryTaskById(User loginUser, String projectName, Integer taskId) {
        Map<String,Object> result = new HashMap<>();
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }
        DepTask depTask = depTaskMapper.selectDepTaskById(taskId);
        if (depTask == null) {
            putMsg(result, Status.DEP_TASK_NOT_EXIST, taskId);
        } else {
            result.put(Constants.DATA_LIST, depTask);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }


    @Override
    public Map<String, Object> queryDepTaskListPaging(User loginUser, String projectName, String searchVal,
                                                      Integer pageNo, Integer pageSize, Integer userId) {

        Map<String, Object> result = new HashMap<>();
        Project project = projectMapper.queryByName(projectName);
        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }
        Page<DepTask> page = new Page<>(pageNo, pageSize);
        IPage<DepTask> depTaskIPage = depTaskMapper.queryDepTaskListPaging(
                page, searchVal, userId, project.getId(), isAdmin(loginUser));
        PageInfo<DepTask> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotalCount((int) depTaskIPage.getTotal());
        pageInfo.setLists(depTaskIPage.getRecords());
        result.put(Constants.DATA_LIST, pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Map<String, Object> queryDepTaskList(User loginUser, String projectName) {
        Map<String,Object> result = new HashMap<>();
        List<DepTask> depTasks = depTaskMapper.queryDepTaskList();
        result.put(Constants.DATA_LIST, depTasks);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    @Transactional(rollbackFor = RuntimeException.class)
    public Map<String, Object> deleteDepTaskById(User loginUser, String projectName, Integer depTaskId) {
        Map<String,Object> result = new HashMap<>();
        DepTask depTask = depTaskMapper.selectDepTaskById(depTaskId);
        Map<String, Object> map = processDefinitionService.deleteProcessDefinitionById(loginUser, projectName,
                depTask.getProcessId());

        Status status = (Status) map.get(Constants.STATUS);
        if(status.getCode() == 0){
            depTaskMapper.deleteById(depTaskId);
            //result.put(Constants.DATA_LIST, processDefineMapper.selectById(processDefine.getId()));
            putMsg(result, Status.SUCCESS);
            result.put(PROCESSDEFINITIONID, depTask.getId());
            return result;
        }else {
            return map;
        }
    }

    @Override
    public Map<String, Object> queryTaskByName(User loginUser, String projectName, String taskNameName) {
        Map<String,Object> result = new HashMap<>();
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, projectName);
        Status resultStatus = (Status) checkResult.get(Constants.STATUS);
        if (resultStatus != Status.SUCCESS) {
            return checkResult;
        }
        DepTask depTask = depTaskMapper.selectDepTaskByName(taskNameName);
        if (depTask == null) {
            putMsg(result, Status.DEP_TASK_NOT_EXIST, taskNameName);
        } else {
            result.put(Constants.DATA_LIST, depTask);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }


    private  String parseLocations(List<Task> nodeList){
        Map<String,Map<String,Object>> locations = new HashMap<>();
        int x = 30;
        int y = 30;
        for (Task node : nodeList) {
            x  = x +    50;
            y = y + 50;
            Map<String,Object> map = new HashMap<>();
            map.put("name",node.getName());
            map.put("targetarr","");
            map.put("nodeNumber",0);
            map.put("x",x);
            map.put("y",y);
            locations.put(node.getId(),map);
        }
        return JSONUtils.toJsonString(locations);

    }

    private String parseConnects(List<Task> nodeList) {
        List<Map<String,String>> connects = new ArrayList<>();
        if(nodeList != null  && nodeList.size() >1){
            Map<String,String> connect = new HashMap<>();
            for (Task task : nodeList) {
                if("DEPENDENT".equals(task.getType())){
                    connect.put("endPointSourceId",task.getId());
                }else {
                    connect.put("endPointTargetId",task.getId());
                }
            }
            connect.put("label","");
            connects.add(connect);
        }
        return JSONUtils.toJsonString(connects);
    }


}
