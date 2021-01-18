package org.apache.dolphinscheduler.api.controller;/*
 * Copyright 2020, Zetyun DEP All rights reserved.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.DepTaskService;
import org.apache.dolphinscheduler.api.service.ProcessDefinitionService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.dao.entity.DepTask;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

import javax.servlet.http.HttpServletResponse;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.BATCH_DELETE_PROCESS_DEFINE_BY_IDS_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.CREATE_PROCESS_DEFINITION;
import static org.apache.dolphinscheduler.api.enums.Status.DELETE_PROCESS_DEFINE_BY_ID_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.QUERY_DATAIL_OF_PROCESS_DEFINITION_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.QUERY_PROCESS_DEFINITION_LIST;
import static org.apache.dolphinscheduler.api.enums.Status.QUERY_PROCESS_DEFINITION_LIST_PAGING_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.RELEASE_PROCESS_DEFINITION_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.UPDATE_PROCESS_DEFINITION_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.VERIFY_PROCESS_DEFINITION_NAME_UNIQUE_ERROR;

@Api(tags = "DEP_TASK_TAG")
@RestController
@RequestMapping("projects/{projectName}/deptask")
public class TaskDefinitionController extends  BaseController{

    private static final Logger logger = LoggerFactory.getLogger(ProcessDefinitionController.class);

    @Autowired
    private ProcessDefinitionService processDefinitionService;

    @Autowired
    private DepTaskService depTaskService;

    /**
     * create process definition
     *
     * @param loginUser login user
     * @param projectName project name
     * @return create result code
     */
    @ApiOperation(value = "save", notes = "CREATE_PROCESS_DEFINITION_NOTES")
    @PostMapping(value = "/save")
    @ResponseStatus(HttpStatus.CREATED)
    @ApiException(CREATE_PROCESS_DEFINITION)
    public Result createProcessDefinition(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                          @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                          @RequestBody DepTask depTask
                                          ) throws JsonProcessingException, InvocationTargetException,
            IllegalAccessException {

        Map<String, Object> result = depTaskService.createTask(loginUser, projectName, depTask);
        return returnDataList(result);
    }



    /**
     * verify process definition name unique
     *
     * @param loginUser login user
     * @param projectName project name
     * @param name name
     * @return true if process definition name not exists, otherwise false
     */
    @ApiOperation(value = "verify-name", notes = "VERIFY_PROCESS_DEFINITION_NAME_NOTES")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "name", value = "PROCESS_DEFINITION_NAME", required = true, type = "String")
    })
    @GetMapping(value = "/verify-name")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(VERIFY_PROCESS_DEFINITION_NAME_UNIQUE_ERROR)
    public Result verifyProcessDefinitionName(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                              @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                              @RequestParam(value = "name", required = true) String name) {
        logger.info("verify process definition name unique, user:{}, project name:{}, process definition name:{}",
                loginUser.getUserName(), projectName, name);
        Map<String, Object> result = processDefinitionService.verifyProcessDefinitionName(loginUser, projectName, name);
        return returnDataList(result);
    }

    /**
     * update process definition
     *
     * @param loginUser login user
     * @param projectName project name
     * @param name process definition name
     * @param id process definition id
     * @param processDefinitionJson process definition json
     * @param description description
     * @param locations locations for nodes
     * @param connects connects for nodes
     * @return update result code
     */

    @ApiOperation(value = "updateProcessDefinition", notes = "更新任务")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "name", value = "PROCESS_DEFINITION_NAME", required = true, type = "String"),
            @ApiImplicitParam(name = "id", value = "PROCESS_DEFINITION_ID", required = true, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "processDefinitionJson", value = "PROCESS_DEFINITION_JSON", required = true, type = "String"),
            @ApiImplicitParam(name = "locations", value = "PROCESS_DEFINITION_LOCATIONS", required = true, type = "String"),
            @ApiImplicitParam(name = "connects", value = "PROCESS_DEFINITION_CONNECTS", required = true, type = "String"),
            @ApiImplicitParam(name = "description", value = "PROCESS_DEFINITION_DESC", required = false, type = "String"),
    })
    @PostMapping(value = "/update")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(UPDATE_PROCESS_DEFINITION_ERROR)
    public Result updateProcessDefinition(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                          @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                          @RequestParam(value = "name", required = true) String name,
                                          @RequestParam(value = "id", required = true) int id,
                                          @RequestParam(value = "processDefinitionJson", required = true) String processDefinitionJson,
                                          @RequestParam(value = "locations", required = false) String locations,
                                          @RequestParam(value = "connects", required = false) String connects,
                                          @RequestParam(value = "description", required = false) String description) {

        logger.info("login user {}, update process define, project name: {}, process define name: {}, "
                        + "process_definition_json: {}, desc: {}, locations:{}, connects:{}",
                loginUser.getUserName(), projectName, name, processDefinitionJson, description, locations, connects);
        Map<String, Object> result = processDefinitionService.updateProcessDefinition(loginUser, projectName, id, name,
                processDefinitionJson, description, locations, connects);
        return returnDataList(result);
    }



    /**
     * release process definition
     *
     * @param loginUser login user
     * @param projectName project name
     * @param taskId task id
     * @param releaseState release state
     * @return release result code
     */
    @ApiOperation(value = "releaseTask", notes = "上线/下线")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "name", value = "PROCESS_DEFINITION_NAME", required = true, type = "String"),
            @ApiImplicitParam(name = "processId", value = "PROCESS_DEFINITION_ID", required = true, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "releaseState", value = "PROCESS_DEFINITION_CONNECTS", required = true, dataType = "Int", example = "100"),
    })
    @PostMapping(value = "/release")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(RELEASE_PROCESS_DEFINITION_ERROR)
    public Result releaseProcessDefinition(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                           @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                           @RequestParam(value = "taskId", required = true) int taskId,
                                           @RequestParam(value = "releaseState", required = true) int releaseState) {

        logger.info("login user {}, release process definition, project name: {}, release state: {}",
                loginUser.getUserName(), projectName, releaseState);
        Map<String, Object> result = depTaskService.releaseTask(loginUser, projectName, taskId, releaseState);
        return returnDataList(result);
    }

    /**
     * query datail of task by id
     */
    @ApiOperation(value = "queryTaskById", notes = "获取任务")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "processId", value = "任务ID", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "/select-by-id")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_DATAIL_OF_PROCESS_DEFINITION_ERROR)
    public Result queryTaskById(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                             @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                             @RequestParam("taskId") Integer taskId
    ) {
        Map<String, Object> result = depTaskService.queryTaskById(loginUser, projectName, taskId);
        return returnDataList(result);
    }

    /**
     * query datail of task  by name
     *
     */
    @ApiOperation(value = "queryTaskByName", notes = "通过任务名获取任务")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "processDefinitionName", value = "PROCESS_DEFINITION_ID", required = true, dataType = "String")
    })
    @GetMapping(value = "/select-by-name")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_DATAIL_OF_PROCESS_DEFINITION_ERROR)
    public Result<ProcessDefinition> queryProcessDefinitionByName(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                                  @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                                                  @RequestParam("taskName") String taskNameName
    ) {
        Map<String, Object> result = depTaskService.queryTaskByName(loginUser, projectName, taskNameName);
        return returnDataList(result);
    }

    /**
     * query Process definition list
     *
     * @param loginUser login user
     * @param projectName project name
     * @return process definition list
     */
    @ApiOperation(value = "queryTaskList", notes = "获取任务列表")
    @GetMapping(value = "/list")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_DEFINITION_LIST)
    public Result queryDepTaskList(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                             @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName
    ) {
        logger.info("query process definition list, login user:{}, project name:{}",
                loginUser.getUserName(), projectName);
        Map<String, Object> result = depTaskService.queryDepTaskList(loginUser, projectName);
        return returnDataList(result);
    }

    /**
     * query process definition list paging
     *
     * @param loginUser login user
     * @param projectName project name
     * @param searchVal search value
     * @param pageNo page number
     * @param pageSize page size
     * @param userId user id
     * @return process definition page
     */
    @ApiOperation(value = "queryTaskListPaging", notes = "获取任务列表分页")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", required = true, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "searchVal", value = "SEARCH_VAL", required = false, type = "String"),
            @ApiImplicitParam(name = "userId", value = "USER_ID", required = false, dataType = "Int", example = "100"),
            @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "/list-paging")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_DEFINITION_LIST_PAGING_ERROR)
    public Result queryTaskListPaging(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                                   @RequestParam("pageNo") Integer pageNo,
                                                   @RequestParam(value = "searchVal", required = false) String searchVal,
                                                   @RequestParam(value = "userId", required = false, defaultValue = "0") Integer userId,
                                                   @RequestParam("pageSize") Integer pageSize) {
        logger.info("query process definition list paging, login user:{}, project name:{}", loginUser.getUserName(), projectName);
        Map<String, Object> result = checkPageParams(pageNo, pageSize);
        if (result.get(Constants.STATUS) != Status.SUCCESS) {
            return returnDataListPaging(result);
        }
        searchVal = ParameterUtils.handleEscapes(searchVal);
        result = depTaskService.queryDepTaskListPaging(loginUser, projectName, searchVal, pageNo, pageSize, userId);
        return returnDataListPaging(result);
    }


    /**
     * delete process definition by id
     *
     * @param loginUser login user
     * @param projectName project name
     * @param processDefinitionId process definition id
     * @return delete result code
     */
    @ApiOperation(value = "deleteTaskById", notes = "通过任务ID 删除任务")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "depTaskId", value = "PROCESS_DEFINITION_ID", dataType = "Int", example = "100")
    })
    @GetMapping(value = "/delete")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(DELETE_PROCESS_DEFINE_BY_ID_ERROR)
    public Result deleteDepTaskById(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                              @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                              @RequestParam("processDefinitionId") Integer processDefinitionId
    ) {
        logger.info("delete process definition by id, login user:{}, project name:{}, process definition id:{}",
                loginUser.getUserName(), projectName, processDefinitionId);
        Map<String, Object> result = depTaskService.deleteDepTaskById(loginUser, projectName, processDefinitionId);
        return returnDataList(result);
    }

    /**
     * batch delete process definition by ids
     *
     * @param loginUser login user
     * @param projectName project name
     * @param processDefinitionIds process definition id list
     * @return delete result code
     */
    @ApiOperation(value = "batchDeleteTaskByIds", notes = "批量删除")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "taskIds", value = "PROCESS_DEFINITION_IDS", type = "String")
    })
    @GetMapping(value = "/batch-delete")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(BATCH_DELETE_PROCESS_DEFINE_BY_IDS_ERROR)
    public Result batchDeleteDepTaskByIds(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                    @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                                    @RequestParam("depTaskIds") String depTaskIds
    ) {


        Map<String, Object> result = new HashMap<>();
        List<String> deleteFailedIdList = new ArrayList<>();
        if (StringUtils.isNotEmpty(depTaskIds)) {
            String[] depTaskIdsIdArray = depTaskIds.split(",");

            for (String deptaskId : depTaskIdsIdArray) {
                int taskId = Integer.parseInt(deptaskId);
                try {
                    Map<String, Object> deleteResult = depTaskService.deleteDepTaskById(loginUser, projectName,
                            taskId);
                    if (!Status.SUCCESS.equals(deleteResult.get(Constants.STATUS))) {
                        deleteFailedIdList.add(deptaskId);
                        logger.error((String) deleteResult.get(Constants.MSG));
                    }
                } catch (Exception e) {
                    deleteFailedIdList.add(deptaskId);
                }
            }
        }

        if (!deleteFailedIdList.isEmpty()) {
            putMsg(result, Status.BATCH_DELETE_PROCESS_DEFINE_BY_IDS_ERROR, String.join(",", deleteFailedIdList));
        } else {
            putMsg(result, Status.SUCCESS);
        }

        return returnDataList(result);
    }

    /**
     * batch export process definition by ids
     *
     * @param loginUser login user
     * @param projectName project name
     * @param processDefinitionIds process definition ids
     * @param response response
     */

    @ApiOperation(value = "batchExportTaskByIds", notes = "批量导出")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "depTaskIds", value = "PROCESS_DEFINITION_ID", required = true, dataType =
                    "String")
    })
    @GetMapping(value = "/export")
    @ResponseBody
    public void batchExportDepTaskByIds(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                  @ApiParam(name = "projectName", value = "PROJECT_NAME", required = true) @PathVariable String projectName,
                                                  @RequestParam("processDefinitionIds") String processDefinitionIds,
                                                  HttpServletResponse response) {
        try {
            logger.info("batch export process definition by ids, login user:{}, project name:{}, process definition ids:{}",
                    loginUser.getUserName(), projectName, processDefinitionIds);
            processDefinitionService.batchExportProcessDefinitionByIds(loginUser, projectName, processDefinitionIds, response);
        } catch (Exception e) {
            logger.error(Status.BATCH_EXPORT_PROCESS_DEFINE_BY_IDS_ERROR.getMsg(), e);
        }
    }

}
