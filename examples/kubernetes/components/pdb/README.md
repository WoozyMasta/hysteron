# components/pdb

Adds PodDisruptionBudget resources for:

* keeper
* sentinel

Optional operational hardening component for controlled voluntary disruption.

> [!CAUTION]
> PDB can block voluntary disruptions (drain/upgrade)
> if replica counts are too low for configured `minAvailable`.
