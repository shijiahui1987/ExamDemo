package com.migu.schedule.node;

import com.migu.schedule.info.TaskInfo;

public class Task extends TaskInfo {
    private int consumption;

    public Task(Integer taskId, Integer consumption) {
        setTaskId(taskId);
        this.consumption = consumption;
    }

    public int getConsumption() {
        return consumption;
    }


}
