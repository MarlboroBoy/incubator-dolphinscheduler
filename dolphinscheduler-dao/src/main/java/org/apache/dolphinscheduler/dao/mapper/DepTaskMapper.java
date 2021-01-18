package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.entity.DepTask;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface DepTaskMapper extends BaseMapper<DepTask> {

    int insertSchedulerTask(DepTask depTask);

    DepTask selectDepTaskById(@Param("taskId") int taskId);

    DepTask selectDepTaskByName(@Param("taskName") String taskName);

    void updateDepTask(DepTask depTask);

    IPage<DepTask> queryDepTaskListPaging(Page<DepTask> page, String searchVal, Integer userId, int id, boolean admin);

    List<DepTask> queryDepTaskList();

}
