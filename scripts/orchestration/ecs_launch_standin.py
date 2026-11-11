"""A stand-in of the Amazon provider's ``EcsRunTaskOperator``, for operator cases that run without Airflow installed.

It mirrors only the parts of ``airflow/providers/amazon/aws/operators/ecs.py`` in apache-airflow-providers-amazon 9.36.0 that the launch operator
subclasses or relies on. Cases that use it are stand-in based: rerun them against the real provider in the Airflow image after any provider upgrade.
``MIRRORED_PROVIDER_VERSION`` is compared with the version pinned in ``services/orchestration/constraints-3.12.txt`` by the ``provider_pin`` case.
"""

from __future__ import annotations

import functools
import logging
from typing import Any
from uuid import NAMESPACE_OID, UUID, uuid5

MIRRORED_PROVIDER_VERSION = "9.36.0"
# operators/ecs.py line 466: ``deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False)``. The real default is read from
# the Airflow configuration when the class is defined. The stand-in reads this setting when an operator is built, so a case can switch it.
OPERATORS_CONFIG = {"default_deferrable": False}


class Deferred(Exception):
    """operators/ecs.py lines 548-568: ``self.defer`` raises a special exception that hands the wait to a trigger, so ``execute`` stops there."""


def generate_uuid(*values: str | None) -> str:
    """aws/utils/identifiers.py lines 27-55: one UUID v5 per value, each the namespace of the next."""
    result = NAMESPACE_OID
    for item in values:
        result = uuid5(result, item if item is not None else str(UUID(int=0)))
    return str(result)


class StandInOperatorMeta(type):
    """airflow/sdk/bases/operator.py ``BaseOperatorMeta.__new__`` (Airflow 3.3.2): ``__init__`` is wrapped only when it differs from the first superclass's.

    A subclass with no ``__init__`` of its own is never wrapped, so the wrapper's check ``func == self.__init__.__wrapped__`` (line 579) raises AttributeError.
    """

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any], **kwargs: Any) -> type:
        new_cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if new_cls.__init__ is not new_cls.mro()[1].__init__:
            new_cls.__init__ = mcs.apply_defaults(new_cls.__init__)
        return new_cls

    @classmethod
    def apply_defaults(mcs, func: Any) -> Any:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            if args:
                raise TypeError("Use keyword arguments when initializing operators")
            if func == self.__init__.__wrapped__:  # bases/operator.py line 579: the outermost wrapped __init__ marks the end of construction
                self._instantiated = True
            return func(self, **kwargs)

        return wrapper


class StandInEcsRunTaskOperator(metaclass=StandInOperatorMeta):
    """The flow of ``EcsRunTaskOperator``: ``execute`` calls the four hooks in this order, and ``on_kill`` stops the task."""

    def __init__(
        self,
        *,
        task_definition: str,
        cluster: str,
        overrides: dict[str, Any],
        reattach: bool = False,
        wait_for_completion: bool = True,
        deferrable: bool | None = None,
        do_xcom_push: bool = False,
        **kwargs: Any,
    ) -> None:
        # operators/ecs.py lines 473-511: the attributes the hooks read.
        self.task_definition = task_definition
        self.cluster = cluster
        self.overrides = overrides
        self.reattach = reattach
        self.wait_for_completion = wait_for_completion
        self.deferrable = OPERATORS_CONFIG["default_deferrable"] if deferrable is None else deferrable
        self.do_xcom_push = do_xcom_push
        self.arn: str | None = None
        self._started_by: str | None = None
        self.log = logging.getLogger("standin_ecs_operator")
        self.client: Any = None

    def execute(self, context: dict[str, Any]) -> None:
        # operators/ecs.py lines 519-590. Left out: the defer call's trigger arguments (550-567), the CloudWatch log fetcher
        # thread (573-582, only with awslogs_group) and the WaiterError clean-up (591-612, the launch operator's wait raises no WaiterError).
        if self.reattach:
            ti = context["ti"]  # lines 535-538: the started-by value is a UUID of the task instance key, and the hook gets it
            self._started_by = generate_uuid(*map(str, [ti.dag_id, ti.task_id, ti.run_id, ti.map_index]))
            self._try_reattach_task(started_by=self._started_by)
        if not self.arn:  # lines 540-543
            self._start_task()
        if self.do_xcom_push:  # lines 545-546
            context["ti"].xcom_push(key="ecs_task_arn", value=self.arn)
        if self.deferrable:  # lines 548-568: the task is handed to a trigger, with no deadline, stop or record of ours
            raise Deferred("TaskDoneTrigger")
        if not self.wait_for_completion:  # lines 570-571
            return
        self._wait_for_task_ended()  # line 584
        self._after_execution()  # line 586

    def _after_execution(self) -> None:
        self._check_success_task()  # lines 637-638

    def _start_task(self) -> None:
        raise NotImplementedError("operators/ecs.py lines 640-691: the provider body is not mirrored, the launch operator replaces it")

    def _try_reattach_task(self, started_by: str) -> None:
        raise NotImplementedError("operators/ecs.py lines 693-706: the provider body is not mirrored, the launch operator replaces it")

    def _wait_for_task_ended(self) -> None:
        raise NotImplementedError("operators/ecs.py lines 708-720: the provider body is not mirrored, the launch operator replaces it")

    def _check_success_task(self) -> None:
        raise NotImplementedError("operators/ecs.py lines 756-803: the provider body is not mirrored, the launch operator replaces it")

    def on_kill(self) -> None:
        # operators/ecs.py lines 805-815: the provider stops the task at once and does not wait for STOPPED.
        if not self.client or not self.arn:
            return
        self.client.stop_task(cluster=self.cluster, task=self.arn, reason="Task killed by the user")
