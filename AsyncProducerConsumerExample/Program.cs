using System.Collections.Concurrent;
using System.Diagnostics.Tracing;
using System.Threading.Tasks.Dataflow;

var job = new Job();
var ctx = new CancellationToken();
var jobTask = job.Execute(ctx);
Task.WaitAny(jobTask, Task.Factory.StartNew(() =>
{
    Console.ReadKey();
    ctx.ThrowIfCancellationRequested();
}));

[EventSource(Name = "Example.ProducerConsumer")]
public sealed class ExecutedTasksCounterSource : EventSource
{
}

public class Job
{
    private const int BufferSize = 10;
    private const int ConsumerCount = 3;

    private readonly ActionBlock<Entity> _action;

    private readonly Producer _producer;
    private readonly ConsumerPool _consumerPool = new(ConsumerCount);

    public Job()
    {
        _action = new ActionBlock<Entity>(
            _consumerPool.Invoke,
            new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = ConsumerCount, BoundedCapacity = BufferSize});

        _producer = new Producer(GetPoolVacancies, _action.SendAsync);
    }

    public async Task Execute(CancellationToken ctx)
    {
        await _producer.Produce(ctx);
        await _action.Completion.WaitAsync(ctx);
    }

    private int GetPoolVacancies()
    {
        var result = BufferSize - _action.InputCount;
        return result;
    }
}

public class Producer
{
    private const int DbPingDelay = 500;
    private const int QueuePingDelay = 50;

    private readonly Func<int> _getFreeCount;
    private readonly Func<Entity, Task<bool>> _tryAddEntity;
    private readonly Db _db = new();

    public Producer(Func<int> getFreeCount, Func<Entity, Task<bool>> tryAddEntity)
    {
        _getFreeCount = getFreeCount;
        _tryAddEntity = tryAddEntity;
    }

    public async Task Produce(CancellationToken ctx)
    {
        while (!ctx.IsCancellationRequested)
        {
            var freeCount = _getFreeCount.Invoke();
            if (freeCount < 1)
            {
                ctx.WaitHandle.WaitOne(QueuePingDelay);
                continue;
            }

            var entities = await _db.GetValues(freeCount);
            if (entities.Count < 1)
            {
                ctx.WaitHandle.WaitOne(DbPingDelay);
                continue;
            }

            foreach (var entity in entities)
            {
                if (!await _tryAddEntity.Invoke(entity))
                {
                    throw new Exception();
                }
            }
        }
    }
}

public class ConsumerPool
{
    private readonly ConcurrentQueue<Consumer> _pool;

    private SpinWait _spin;

    public ConsumerPool(int poolSize)
    {
        _pool = new ConcurrentQueue<Consumer>(
            Enumerable.Repeat(0, poolSize).Select(_ => new Consumer()));
    }

    public async Task Invoke(Entity entity)
    {
        Consumer? consumer;
        while (!_pool.TryDequeue(out consumer))
        {
            _spin.SpinOnce();
        }

        await consumer.Invoke(entity);
        _pool.Enqueue(consumer);
    }
}

public class Consumer
{
    private readonly Guid _id = Guid.NewGuid();

    public async Task Invoke(Entity entity)
    {
        await Task.Delay(50);
        Console.WriteLine($"Value: '{entity.Value.ToString()}'. Id: '{_id.ToString()}'");
    }
}

public class Db
{
    private int _currentValue = 0;

    public Task<IReadOnlyList<Entity>> GetValues(int count)
    {
        IReadOnlyList<Entity> result = Enumerable.Repeat(0, count)
            .Select(_ => new Entity(++_currentValue)).ToList();
        Console.WriteLine($"Db returned {result.Count} entities");
        return Task.FromResult(result);
    }
}

public record Entity(int Value);