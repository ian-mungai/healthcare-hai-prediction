"""The drift gate of one model: after its feature set is published, train when the model drifted, its feature schema changed or it has no Production model.

The gate reads what the drift task already produced for the model: which types the baseline phase could monitor and why it could not monitor the others
(``baseline.fetch``), and the drift record of each monitored type. A type trains when

- the baseline phase found no Production model of this model for it (``no_production_model``, ``production_model_of_another_model``, ``no_features``),
- the feature schema of the model differs from the one its Production model trained on (``feature_schema_changed``), or
- its drift record says ``triggered`` (at least half of the model's features drifted, the rule of ``config/monitoring/drift.json``).

Every other type records a skip with the reason. A feature with no current values and a current window under the minimum rows refuse earlier, in
``drift_check``, and so refuse the whole decision by name. The decision carries the types to train and, when there are none, the reason for each skip.

The rule itself is ``orchestration.training_contract.decide_drift_gate``: the Airflow container mounts that module and not this package, and the
drift DAG recomputes the decision from the marker's records with it before it starts a training run.
"""

from orchestration.training_contract import TRAIN_WITHOUT_CHECK, GateRefused, decide_drift_gate

decide = decide_drift_gate

__all__ = ["TRAIN_WITHOUT_CHECK", "GateRefused", "decide"]
