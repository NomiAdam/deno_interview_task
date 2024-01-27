const PORT = 8080;
const MAX_QUEUE_SIZE = 4;

interface TasksQueueRequestBody extends Record<string, number> {}

interface Status {
  pending: Task[];
  running: Task[];
}

class Task {
  key: string;
  duration: number;

  constructor(key: string, duration: number) {
    this.key = key;
    this.duration = duration;
  }

  run(onDone: VoidFunction) {
    console.log(`Task ${this.key} started. Running for ${this.duration}ms`);
    setTimeout(onDone, this.duration);
  }
}

class TaskScheduler {
  private pending: Task[] = [];
  private running: Task[] = [];

  scheduleTask(task: Task) {
    if (this.running.length >= MAX_QUEUE_SIZE) {
      console.log(`Max queue size exceeded, ${task.key} is pending`);
      this.pending.push(task);
      return;
    }
    if (this.running.some(({ key }) => key === task.key)) {
      console.log(`Task ${task.key} is already running. Queuing`);
      this.pending.push(task);
      return;
    }
    this.running.push(task);
    task.run(() => {
      console.log(`Task ${task.key} finished in ${task.duration}`);

      const runningTaskIndex = this.running.indexOf(task);

      if (runningTaskIndex > -1) {
        this.running.splice(runningTaskIndex, 1);
      }
      this.scheduleNextTask();
    });
  }

  scheduleNextTask() {
    console.log("Scheduling next task");
    if (this.pending.length === 0) {
      console.log("No pending tasks");
      return;
    }
    const nextViableTaskIndex = this.pending.findIndex(
      ({ key }) => !this.running.some((runningTask) => runningTask.key === key)
    );
    if (nextViableTaskIndex > -1) {
      const task = this.pending[nextViableTaskIndex];
      this.pending.splice(nextViableTaskIndex, 1);
      this.scheduleTask(task);
    } else {
      console.log("No viable next task found");
    }
  }

  append(tasks: Task[]) {
    console.log(`Apending ${tasks.length} tasks`);
    tasks.forEach((task) => this.scheduleTask(task));
  }

  status(): Status {
    return {
      pending: this.pending,
      running: this.running,
    };
  }
}

const taskScheduler = new TaskScheduler();

const handler = async (request: Request): Promise<Response> => {
  if (request.method === "GET" && request.url.endsWith("/queue/status")) {
    return new Response(JSON.stringify(taskScheduler.status()), {
      status: 200,
      headers: {
        "content-type": "application/json",
      },
    });
  }

  if (request.method === "POST" && request.url.endsWith("/queue/tasks")) {
    try {
      const body: TasksQueueRequestBody = await request.json();
      taskScheduler.append(
        Object.entries(body).reduce((acc: Task[], [key, value]) => {
          acc.push(new Task(key, value));
          return acc;
        }, [])
      );
      return new Response("ok", { status: 200 });
    } catch (e) {
      return new Response("nok", { status: 500 });
    }
  }

  return new Response("", { status: 404 });
};

console.log(`HTTP server running. Access it at: http://localhost:${PORT}/`);
Deno.serve({ port: PORT }, handler);
