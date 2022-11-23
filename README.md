# matlab_mym_parallel
MATLAB class for managing parallel computing jobs in MATLAB using MYSQL

MATLAB often does not fully use the computing power of modern multi core systems. However, when running multiple MATLAB instances in parallel, one needs a synchronization method to split the workload. This functionality is provided by the "Parallel Computing Toolbox", e.g. by using "parfor" instead of "for". The "matlab_mym_parallel" provides a simple workaround for parallel computing where the synchronization is done through a MySQL database which implements a distributed scheduler. Multiple MATLAB instances can then perform different parts of the computation, fully using all available cores or even multiple computers on a network.

This class requires the MySQL API for MATLAB to be installed. You can get it from:
   https://github.com/datajoint/mym

## Setup:

### MySQL Setup

* Install MySQL on your system and make sure the service is running
* Login as root e.g. by `sudo mysql -u root`
* Create a new database and user:

```
CREATE DATABASE mym_parallel;
CRAETE USER 'mym'@'localhost' IDENTIFIED WITH mysql_native_password BY 'eik2ienie4oo';
GRANT ALL PRIVILEGES ON mym_parallel.* TO 'mym'@'localhost' WITH GRANT OPTION;
EXIT
```
If you change these settings, manage multiple users or connect to the database through the network, you must edit the `mym_parallel_login.txt` for each user.


### Install MATLAB MySQL API 
The source code can be found at: from https://github.com/datajoint/mym
The easiest way to do so is by using the MATLAB built-in GUI (requires R2016b or later):

1. Utilize MATLAB built-in GUI i.e. *Top Ribbon -> Add-Ons -> Get Add-Ons*
2. Search and Select `mym`
3. Select *Add from GitHub*

### Install "matlab_mym_parallel"
Simply add the `matlab_mym_parallel` folder to your MATLAB path. Edit the credentials in the `mym_parallel_login.txt` if needed.


## Simple Example

In order to use the distributed scheduler, you must have a valid login on the database. You can then create a new task by calling either on the workstation or on your desktop PC:
```
job = mym_parallel.create( no_wps, task_id, login_file )
```
This specifies a number of work packages and a task id. The "task_id" must be a unique number that identifies the computing task. You need to include this id in your evaluation script (which runs in parallel) in order to connect to the correct scheduler. The "login_file" is optional. If it is not specifies, the default file "mym_parallel_login.txt" is used. You then have to connect to scheduler by:
```
job = mym_parallel( task_id, login_file )
```
On the computing node, you can then call one of the following:
```
wp = job.get_wp
wp = job.get_wp( 'random' )
```
This returns a unique number between 1 and "no_wps". After you finished you calculations, call
```
job.set_finished
```
to indicate the the current WP has ended. Then you can call the next WP. A typical parallel script then looks like:
```
job = mym_parallel( task_id )     % Connect
while job.get_wp                  % Obtain WP
   wp = job.wp;                   % WP number

   % Your computations

  job.set_finished;
end
if job.is_finished         % Runs only once

   % Postprocessing

end
exit
```
## Advanced features
### WP status
Each WP has 5 different states. Those can be seen when calling "job.status". The states are indicated by a number from 0-4 with the meaning:
* `(0)` he WP is ready to run. The lowest WP number with "status=0" will be returned when calling "get_wp". In this case, the state of the WP is changed to "1".
* `(1)` The WP is running. The state is changes to "2" by calling "job.set_finished".
* `(2)` The WP is finished.
* `(3)` All WPs are finished. This state is set by calling "job.is_finished" for the first time after all WPs are in state "2".
* `(4)` The WP is suspended. It has not started yet but will also not be returned by calling "job.get_wp".

### Suspending and resetting

* `job.suspend` - You can suspend all ready WPs (status=0) by calling "job.suspend". In this case, all running WPs will finish and no new WPs will be returned by calling "job.get_wp".

* `job.reset` - Sets all running WPs (status=1) to ready (status=0), so they can be run again if "get_wp" is called. This is useful when a WP crashed and thus did not call "job.set_finished". Hence, it will always remain in running state.

* `job.reset('all')` - Sets all WPs (status=1|2|3|4) to ready (status=0).

### Locking
When you call "`job.lock`", the job enters a single-thread section. Only one WP can hold a lock at any given time. Hence, the code between calling "`job.lock`" and "`job.unlock`" does not run in parallel on several nodes. This can be used, for example, to synchronize write-access to a file where only one WP can write at a given time. If a seconds WP calls "`job.lock`" while the lock is already owned by another WP, the second WP will wait until the lock is released.









