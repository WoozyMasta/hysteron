# PostgreSQL upgrade

This is an example of upgrading a hysteron cluster from pg9.6 to pg10.0. It use pg_upgrade to
perform the upgrade.

An alternative way to upgrade would be to dump (pg_dump) and restore the database on a new
hysteron cluster. This may be easier to perform, but on large database result in longer downtime.

The tricky part of the upgrade, is that pg_upgrade require both PostgreSQL 9.6 and
PostgreSQL 10.0 binary to do the upgrade. Major upgrade of PostgreSQL on Docker is discussed
on https://github.com/docker-library/postgres/issues/37, this example use the
[tianon/docker-postgres-upgrade](https://github.com/tianon/docker-postgres-upgrade) solution.


## Upgrade with pg_upgrade

As usual, before processing with major upgrade, it is recommended to perform a backup of the database.

pg_upgrade require exclusive access to data files, shutdown the old PostgreSQL server.

```
kubectl delete -f hysteron-keeper.yaml
```

Note: since hysteron-keeper.yaml only contains the StatefulSet/hysteron-keeper object, only this objects
and the created pods will be deleted. Not the persistent volume claim that contains data.

Run a pod with `tianon/postgres-upgrade:9.6-to-10` on keeper-0 data and attach it:

```
cat << EOF | kubectl create -f -
kind: Pod
apiVersion: v1
metadata:
  name: hysteron-upgrade
spec:
  volumes:
    - name: data-hysteron-keeper-0
      persistentVolumeClaim:
       claimName: data-hysteron-keeper-0
  containers:
    - name: hysteron-upgrade
      args:
      - bash
      stdin: true
      tty: true
      image: tianon/postgres-upgrade:9.6-to-10
      volumeMounts:
      - mountPath: "/hysteron-data"
        name: data-hysteron-keeper-0
EOF

kubectl attach -ti hysteron-upgrade
```

Inside this hysteron-upgrade pod, create a hysteron user that will run pg_upgrade (pg_upgrade refuse
to run as root):

```
useradd --uid 1000 hysteron
gosu hysteron bash
```

pg_upgrade work by "copying" data from old PostgreSQL to new PostgreSQL (applying required
change). It needs two PostgreSQL data folder. When option `--link` is used pg_upgrade will
not copy data but use hard-link to speed up process. Refer to
[pg_upgrade documentation](https://www.postgresql.org/docs/current/static/pgupgrade.html) for
more details.

Create the new PostgreSQL data folder:

```
PGDATA=/hysteron-data/postgres-new initdb
```

pg_upgrade will start old and new database and require access to them. Use new pg_hba to allow
local access for pg_upgrade:

```
cp /hysteron-data/postgres-new/pg_hba.conf /hysteron-data/postgres/pg_hba.conf
```

Run pg_upgrade

```
cd /tmp
pg_upgrade -d /hysteron-data/postgres -D /hysteron-data/postgres-new --link
```

Move postgres-new folder in place of postgres folder:

```
rm -fr /hysteron-data/postgres
mv /hysteron-data/postgres-new/ /hysteron-data/postgres
```

pg_upgrade said that two script were generated (at least for 9.6 to 10.0):

* One to remove old data: it was done by the above move
* Another to update statistics (using vacuumdb once PostgreSQL will be running). This
  will be run later.

Exit and delete this hysteron-upgrade pod:

```
kubectl detele pod hysteron-upgrade
```

For all other data volume of hysteron-keeper, run the same step. Just update the yaml used in
kubectl create. Tip: `kubectl get pvc` will list all persistent volume claim, thus all
data volume that needs update.


Once all data volume are updated, re-create the hysteron-keeper using PostgreSQL 10 image:

```
sed -i 's/hysteron:master-pg9.6/hysteron:master-pg10.0/' hysteron-keeper.yaml
kubectl create -f hysteron-keeper.yaml
```

The cluster should quickly resume its operation. Once done, find the current master and
run the vaccumdb as pg_upgrade said:

```
kubectl exec -ti hysteron-keeper-0 bash  # this assume -0 is the master
su - hysteron
vacuumdb --all --analyze-in-stages -h 127.0.0.1 -U hysteron
```

