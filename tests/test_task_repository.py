"""TaskRepository 测试"""

import pytest

from src.core.task import Task, TaskStatus
from src.services.task_repository import InMemoryTaskRepository


@pytest.fixture
def repo():
    return InMemoryTaskRepository()


@pytest.fixture
def sample_task():
    return Task(
        name="测试任务",
        pipeline_name="steam_basic",
        collector_name="steam",
        status=TaskStatus.PENDING,
    )


@pytest.mark.asyncio
async def test_save_and_load(repo, sample_task):
    """保存后应能按 ID 加载"""
    await repo.save(sample_task)
    loaded = await repo.load(sample_task.id)
    assert loaded is not None
    assert loaded.id == sample_task.id
    assert loaded.name == "测试任务"


@pytest.mark.asyncio
async def test_load_missing_returns_none(repo):
    """加载不存在的任务应返回 None"""
    result = await repo.load("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_delete(repo, sample_task):
    """删除后应无法加载"""
    await repo.save(sample_task)
    success = await repo.delete(sample_task.id)
    assert success is True
    assert await repo.load(sample_task.id) is None


@pytest.mark.asyncio
async def test_delete_missing(repo):
    """删除不存在的任务返回 False"""
    success = await repo.delete("nonexistent")
    assert success is False


@pytest.mark.asyncio
async def test_query_all(repo):
    """query 返回所有任务"""
    for i in range(5):
        task = Task(name=f"任务{i}", pipeline_name="test")
        await repo.save(task)
    tasks = await repo.query(limit=10)
    assert len(tasks) == 5


@pytest.mark.asyncio
async def test_query_pagination(repo):
    """query 支持 offset/limit"""
    for i in range(10):
        task = Task(name=f"任务{i}", pipeline_name="test")
        await repo.save(task)
    tasks = await repo.query(limit=3, offset=0)
    assert len(tasks) == 3
    tasks2 = await repo.query(limit=3, offset=3)
    assert len(tasks2) == 3


@pytest.mark.asyncio
async def test_query_by_status(repo):
    """query_by_status 按状态过滤"""
    running = Task(name="运行中", pipeline_name="test", status=TaskStatus.RUNNING)
    success = Task(name="已完成", pipeline_name="test", status=TaskStatus.SUCCESS)
    failed = Task(name="失败", pipeline_name="test", status=TaskStatus.FAILED)

    await repo.save(running)
    await repo.save(success)
    await repo.save(failed)

    result = await repo.query_by_status(TaskStatus.RUNNING)
    assert len(result) == 1
    assert result[0].status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_list_keys(repo, sample_task):
    """list_keys 返回任务键列表"""
    await repo.save(sample_task)
    keys = await repo.list_keys()
    assert len(keys) == 1
    assert keys[0].startswith("task:")


@pytest.mark.asyncio
async def test_list_keys_prefix_filter(repo, sample_task):
    """list_keys 支持前缀过滤"""
    await repo.save(sample_task)
    keys = await repo.list_keys(prefix="task:")
    assert len(keys) == 1
    keys_empty = await repo.list_keys(prefix="nonexistent:")
    assert len(keys_empty) == 0


@pytest.mark.asyncio
async def test_count_by_status(repo):
    """按状态统计任务数量"""
    await repo.save(Task(name="t1", pipeline_name="test", status=TaskStatus.PENDING))
    await repo.save(Task(name="t2", pipeline_name="test", status=TaskStatus.PENDING))
    await repo.save(Task(name="t3", pipeline_name="test", status=TaskStatus.RUNNING))

    counts = await repo.count_by_status()
    assert counts.get("pending") == 2
    assert counts.get("running") == 1


@pytest.mark.asyncio
async def test_save_updates_existing(repo, sample_task):
    """保存同一 task_id 的任务应更新"""
    await repo.save(sample_task)
    sample_task.status = TaskStatus.RUNNING
    await repo.save(sample_task)

    loaded = await repo.load(sample_task.id)
    assert loaded is not None
    assert loaded.status == TaskStatus.RUNNING
