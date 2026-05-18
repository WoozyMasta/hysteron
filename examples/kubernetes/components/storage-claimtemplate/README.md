# components/storage-claimtemplate

Switches keeper storage to StatefulSet `volumeClaimTemplates`.

This is an example storage mode component.
Validate StorageClass and reclaim policy before production usage.

> [!CAUTION]
> Changing storage mode of an existing keeper StatefulSet is a migration task;
> plan data movement and rollback explicitly.
