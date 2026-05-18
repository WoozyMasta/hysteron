# components/storage-pvc

Switches keeper storage to a standalone pre-created PVC model.

This is an example storage mode component.
A single PVC cannot be shared safely by multiple keeper replicas,
so review replica/storage behavior before production usage.

> [!WARNING]
> Example storage mode component.
> A single standalone PVC cannot be shared safely by multiple keeper replicas.
> Validate replica/storage behavior before production usage.
