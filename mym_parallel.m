classdef mym_parallel < handle
    % MYM_PARALLEL Class for managing parallel computing jobs in MATLAB using MYSQL
    %
    % Setup:
    %   The "mym_parallel" class requires a working Matlab interface to MySQL server ("mym") in oder
    %   to work. For this, you need to install the MySQL API for MATLAB from:
    %       https://github.com/datajoint/mym
    %
    %   In addition, you need a login-file containing the login-data.
    %   This file must be a text-file having the content:
    %       db_server   = [ Servername or IP address ]
    %       db_database = [ Database name ]
    %       db_user     = [ User Name ]
    %       db_passwd   = [ Password ]
    %       db_table    = [ Table Name (optional) ]
    %
    %   There are two options to use this file:
    %       1. It must be named "mym_parallel_login.txt" and on you MATLAB seach path
    %       2. You can specify the path to this file when calling "mym_parallel" or
    %          "mym_parallel.create" 
    %
    % Simple Example:
    %   In order to use the scheduler, you must have a valid login on the database. You can then
    %   create a new task by calling either on the workstation or on your desktop PC:
    %
    %       job = mym_parallel.create( no_wps, task_id, 'login_file' )
    %
    %   The "task_id" must be a unique integer number. You need to include this id in your
    %   evaluation script (which runs on the grid) in order to connect to the correct scheduler. On
    %   the computing grid you then have to connect to scheduler by:
    %
    %       job = mym_parallel( task_id, 'login_file' )
    %
    %   On the computing node, you can then call one of the following:
    %
    %       wp = job.get_wp
    %       wp = job.get_wp( 'random' )
    %
    %   This returns a unique number between 1 and "no_wps". After you finished you calculations,
    %   call:
    %
    %       job.set_finished
    %
    %   to indicate the the current WP has ended. Then you can call the next WP. A typical script on
    %   the grid can look like: 
    %
    %       job = mym_parallel( task_id, 'user', 'pass' )     % Connect
    %       while job.get_wp            % Obtain WP
    %           wp = job.wp;            % WP number
    %
    %           % Your computations
    %
    %           job.set_finished;
    %       end
    %       if job.is_finished          % Runs only once
    %
    %           % Postprocessing
    %
    %       end
    %       exit
    %
    %
    % Methods and advanced features:
    %
    % WP status:
    %
    %   Each WP has 5 different states. Those can be seen when calling "job.status". The states are
    %   indicated by a number from 0-4 with the meaning:
    %   0   The WP is ready to run. The lowest WP number with "status=0" will be returned when
    %   calling "get_wp". In this case, the state is changed to "1".
    %   1   The WP is running. The state is changes to "2" by calling "job.set_finished".
    %   2   The WP is finished.
    %   3   All WPs are finished. This state is set by calling "job.is_finished" for the first time
    %       after all WPs are in state "2". 
    %   4   The WP is suspended. It has not started yet but will also not be returned by calling
    %       "job.get_wp". 
    %
    %
    % Suspending and resetting:
    %
    %   job.suspend
    %   You can suspend all ready WPs (status=0) by calling "job.suspend". In this case, all running
    %   WPs will finish and no new WPs will be returned by calling "job.get_wp".
    %
    %   job.reset
    %   This sets all suspended WPs (status=4) back to ready (status=0).
    %
    %   job.reset('running')
    %   Sets all running WPs (status=1) to ready (status=0), so they can be run again if "get_wp" is
    %   called. This is useful when a WP crashed and thus did not call "job.set_finished". Hence, it
    %   will always remain in running state.
    %
    %   job.reset('all')
    %   Sets all WPs (status=1|2|3|4) to ready (status=0). Dont forget to call  this function before
    %   resubmitting the Task to the grid. 
    %
    %
    % Creation and connection:
    %
    %   You can specify a different database server and database by:
    %
    %   [job,task] = create( no_wps, task_number, db_user, db_passwd, db_table, db_server, db_database )
    %   job = mym_parallel( task, db_user, db_passwd, db_table, db_server, db_database  )
    %
    %
    % Locking:
    %
    %   When you call "job.lock", the job enters a single-thread section. Only one WP can hold a
    %   lock at any given time. Hence, the code between calling "job.lock" and "job.unlock" does not
    %   run in parallel on several nodes. This can be used, for example, to synchronize write-access
    %   to a file where only one WP can write at a given time. If a seconds WP calls "job.lock"
    %   while the lock is already owned by another WP, the second WP will wait.
    %
    % Author: Stephan Jaeckel
    % info@sjc-wireless.com
    
    properties(Dependent)
        task                                % The task-id
    end
    
    properties(Dependent,SetAccess=protected)
        last_update                         % The last update of the information
    end
    
    properties(SetAccess=protected)
        wp = 0;                             % The workpackage id
        
        % Indicates if the current WP has the exclusive lock.
        % You can call "job.lock" to obtain it.
        has_lock = false;
        
        % The total number of workpackages for the current task
        wp_total
        
        % The total of unprocessed workpackages
        wp_todo
        
        % The total number of running workpackages
        % This get increased by 1 if "get_wp" is called
        wp_running
        
        % The total number of running workpackages
        % This get increased by 1 if "set_finished" is called
        wp_finished
        
        % The status of the WPs. "0" is for pending. "1" is for running.
        % "2" is for finished.
        status
    end
    
    properties(Dependent)
        depends                             % The Dependencies of the WPs
    end
    
    properties
        login_file_name = '';               % The login-file
        db_user     = '';                   % The MySQL user name
        db_passwd   = '';                   % The MySQL password
        db_table    = '';                   % The MySQL table
        db_server   = '';                   % The server name or IP-Address
        db_database = '';                   % The MySQL database
    end
    
    properties(Access=private)
        Ptask
        Pdepends
    end
    
    methods
        
        % Constructor
        % The constructor is used to conect to an existing task
        function obj = mym_parallel( task, login_file_name  )
            if exist('login_file_name','var')
                login_data              = obj.parse_login_file( login_file_name );
                obj.login_file_name     = login_file_name;
            else
                login_data              = obj.parse_login_file;
                obj.login_file_name     = obj.login_file;
            end
            
            obj.db_user     = login_data.db_user;
            obj.db_passwd   = login_data.db_passwd;
            obj.db_server   = login_data.db_server;
            obj.db_database = login_data.db_database;
            
            if isempty( login_data.db_table )
                obj.db_table    = login_data.db_user;
            else
                obj.db_table    = login_data.db_table;
            end
            
            if exist('task','var')
                if ~isempty( task )
                    obj.task = task;
                end
            end
        end
        
        function out = get.last_update(obj)
            obj.update;
            out = datestr(now);
        end
        
        function out = get.task(obj)
            out = obj.Ptask;
        end
        
        function out = get.depends(obj)
            out = obj.Pdepends;
        end
        
        function set.depends(obj,value)
            if ~exist('value','var') || isempty(value)
                error('No dependencies given.')
            end
            value = value(:);
            if numel(value) ~= obj.wp_total
                error('Dependencies must be a vector having the same size as number of WPs.')
            end
            if ~isreal(value) || any( value < 0 ) || any( value > obj.wp_total )
                error('Dependencies can not exceed the number of WPs.')
            end
            
            mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
            upd = mym(['select job,depend from ',obj.db_table,...
                ' where task=',ml(obj.task),' order by wp']);
            for n = 1:obj.wp_total
                if value(n) == n
                    error('A job can not depend on itself.');
                elseif upd.depend(n) ~= value(n)
                    mym(['update ',obj.db_table,' set depend=',ml(value(n)),' where ',...
                        'job=',ml( upd.job(n) )] );
                end
            end
            mym('close');
        end
        
        function set.task(obj,value)
            if exist('value','var') && ~isempty(value)
                obj.Ptask = value;
            else
                obj.Ptask = [];
            end
            obj.update;
        end
        
        function update( obj )
            % UPDATE Updates the statistics from the server
            
            if isempty( obj.task )
                obj.wp_total = [];
                obj.wp_todo = [];
                obj.wp_running = [];
                obj.wp_finished = [];
                
            else
                mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
                stat = mym(['select status, depend from ',obj.db_table,....
                    ' where task=',ml(obj.task),' order by wp']);
                mym('close');
                
                obj.wp_total = numel( stat.status );
                obj.wp_todo = sum( stat.status == 0 );
                obj.wp_running = sum( stat.status == 1 );
                obj.wp_finished = sum( stat.status == 2 | stat.status == 3  );
                obj.status = stat.status.';
                obj.Pdepends = stat.depend.';
            end
        end
        
        function wp = get_wp( obj, usage )
            % GET_WP Gets the next unprocessed WP
            %
            %   This method returns the next unprocessed WP number
            %   (status=0) and sets the status to running (status=1).
            %
            %   get_wp( 'random' )
            %   returns a random unprocessed WP number.
            %
            %   If there are unpreocessed WP that are dependent on the
            %   completion of another WP, then this function waits until it
            %   can either get a WP or all WPs are done.
            
            if ~exist('usage','var') || isempty(usage)
                usage = 'default';
            end
            
            did_not_get_wp = true;
            while did_not_get_wp
                
                mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
                mym(['LOCK TABLES ',obj.db_table,' WRITE']);
                
                switch usage
                    case 'default'
                        wp = mym(['select job, wp from ',obj.db_table,...
                            ' where task=',ml(obj.task),' && status=0 && depend=0 ',...
                            ' order by wp limit 1']);
                        
                    case 'random'
                        wp = mym(['select job, wp from ',obj.db_table,...
                            ' where task=',ml(obj.task),' && status=0 && depend=0 ',...
                            ' order by wp']);
                        if ~isempty( wp.wp )
                            ii = randi( numel( wp.wp ) );
                            wp.job = wp.job( ii );
                            wp.wp  = wp.wp( ii );
                        end
                        
                    otherwise
                        mym('UNLOCK TABLES');
                        mym('close');
                        error('Usage must be "default" or "random".')
                end
                
                if ~isempty( wp.wp )
                    mym(['update ',obj.db_table,' set status=1 where job=',ml(wp.job)] );
                    mym('UNLOCK TABLES');
                    mym('close');
                    
                    wp = wp.wp;
                    did_not_get_wp = false;
                else
                    wps_left = mym(['select count(*) as ct from ',obj.db_table,...
                        ' where task=',ml(obj.task),' && status=0']);
                    mym('UNLOCK TABLES');
                    mym('close');
                    
                    if wps_left.ct == 0
                        wp = 0;
                        did_not_get_wp = false;
                    else
                        pause(2+rand*8);
                    end
                end
            end
            
            obj.wp = wp;
        end
        
        function set_finished( obj, wp )
            % SET_FINISHED Sets the status of the current WP to finished
            %
            %   SET_FINISHED
            %   Sets the status of the current WP to finished (status=2).
            %   Releases any any lock owned by the current WP.
            %
            %   SET_FINISHED( wp )
            %   Optionally, you can specify the WP number that should be
            %   marked as finished as input variable.
            
            if ~exist('wp','var')
                wp = obj.wp;
            end
            
            if wp == 0
                error('No WP is active.');
            end
            
            mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
            mym(['update ',obj.db_table,...
                ' set status=2 where task=',ml(obj.task),' && wp in(',ml(wp),')'] );
            
            mym(['update ',obj.db_table,...
                ' set depend=0 where task=',ml(obj.task),' && depend in(',ml(wp),')'] );
            
            if obj.has_lock
                obj.unlock;
            end
            obj.wp = 0;
            
            mym('close');
        end
        
        function reset( obj, all, wp_no )
            % RESET Resets the status of the WPs.
            %
            %   RESET
            %   Sets all suspended WPs (status=4) to ready (status=0).
            %
            %   RESET( 'running' )
            %   Sets all running WPs (status=1) to ready (status=0), so
            %   they can be run again if "get_wp" is called.
            %   All locks are released.
            %
            %   RESET( 'all' )
            %   Sets all WPs (status=1|2|3|4) to ready (status=0).
            %   All locks are released.
            %
            %   RESET( 'wp',wp_no )
            %   Sets the specified WP number(s) to ready (status=0).
            %   "wp_no" can be a vector of wps that sould be resettet.
            
            mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
            
            if exist('all','var') && strcmp( all,'running' )
                % Reset running WPs and release locks.
                mym(['update ',obj.db_table,...
                    ' set status=0, block=0 where task=',ml(obj.task),' && status=1'] );
                obj.wp = 0;
                obj.has_lock = 0;
                
            elseif exist('all','var') && strcmp( all,'all' )
                % Reset all WPs and release locks.
                mym(['update ',obj.db_table,...
                    ' set status=0, block=0 where task=',ml(obj.task)] );
                obj.wp = 0;
                obj.has_lock = 0;
                
            elseif exist('all','var') && strcmp( all,'wp' )
                % Reset specific WP
                if ~exist('wp_no','var')
                    error('You must specify a WP number.')
                else
                    mym(['update ',obj.db_table,...
                        ' set status=0, block=0 where task=',ml(obj.task),' && wp in(',ml(wp_no),')'] );
                end
                
            else
                % Reset suspended WPs
                mym(['update ',obj.db_table,...
                    ' set status=0 where task=',ml(obj.task),' && status=4'] );
            end
            
            mym('close');
        end
        
        function suspend( obj )
            % SUSPEND Suspends all ready jobs
            %
            %   This method sets all ready WPs (status=0) to suspended
            %   (status=4). Hence, all running WPs will exit normally and
            %   then the task will halt. To resume the task call
            %   "job.reset".
            
            mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
            mym(['update ',obj.db_table,...
                ' set status=4 where task=',ml(obj.task),' && status=0'] );
            mym('close');
        end
        
        function delete_task( obj )
            % DELETE_TASK Deletes the current task from the database
            
            if ~isempty( obj.task )
                
                mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
                
                mym(['delete from ',obj.db_table,...
                    ' where task=',ml(obj.task)] );
                obj.task = [];
                
                mym('close');
            end
        end
        
        function out = is_finished( obj )
            % IS_FINISHED Checks if the task is finished
            %
            %   If all WPs have finished, this function returns "true".
            %   However, it returns "true" only when it is called for the
            %   first time. You can use this method to run post processing
            %   routines for the entire task.
            
            mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
            
            mym(['LOCK TABLES ',obj.db_table,' WRITE']);
            stat = mym(['select status from ',obj.db_table,...
                ' where task=',ml(obj.task),'']);
            
            if all( stat.status == 2 )
                out = true;
                mym(['update ',obj.db_table,...
                    ' set status=3 where task=',ml(obj.task)] );
            else
                out = false;
            end
            mym('UNLOCK TABLES');
            mym('close');
        end
        
        function lock( obj )
            % LOCK Enters an exclusive section
            %
            %   All code that runs in between "lock" and "unlock" is
            %   exclusive to the current WP. Only one WP can hold the lock.
            %   If another WP tries to obtain the lock, it has to wait
            %   until "unlock" is called by the holder. This can be used to
            %   synchronize e.g. writing to a file.
            
            if obj.wp == 0
                error('Lock can not be acquired. No WP is active.')
            else
                
                has_lock1 = false;
                while ~has_lock1
                    mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
                    mym(['LOCK TABLES ',obj.db_table,' WRITE']);
                    lock = mym(['select block from ',obj.db_table,...
                        ' where task=',ml(obj.task),' && block=1']);
                    if isempty( lock.block )
                        mym(['update ',obj.db_table,...
                            ' set block=1 where task=',ml(obj.task),...
                            ' && wp=',ml(obj.wp)] );
                        mym('UNLOCK TABLES');
                        has_lock1 = true;
                        obj.has_lock = true;
                    else
                        mym('UNLOCK TABLES');
                    end
                    mym('close');
                    
                    if ~has_lock1
                        pause(10);
                    end
                end
            end
        end
        
        function unlock( obj , force )
            % UNLOCK Releases the lock
            %
            %   If you call "unlock('force')", you can steal the lock from
            %   another WP.
            
            mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
            
            if exist('force','var') && strcmp(force,'force')
                mym(['LOCK TABLES ',obj.db_table,' WRITE']);
                mym(['update ',obj.db_table,...
                    ' set block=0 where task=',ml(obj.task),'']);
                mym('UNLOCK TABLES');
            else
                mym(['LOCK TABLES ',obj.db_table,' WRITE']);
                lock = mym(['select job,block from ',obj.db_table,...
                    ' where task=',ml(obj.task),' && wp=',ml(obj.wp)]);
                
                if lock.block == 1
                    mym(['update ',obj.db_table,...
                        ' set block=0 where job=',ml(lock.job)]);
                    mym('UNLOCK TABLES');
                else
                    mym('UNLOCK TABLES');
                    error('The current WP does not own the lock.')
                end
            end
            obj.has_lock = false;
            
            mym('close');
        end
        
        function task_id = create_task_id(obj)
            
            mym_connect(obj.db_server, obj.db_user, obj.db_passwd, obj.db_database);
            
            mym(['LOCK TABLES ',obj.db_table,' WRITE']);
            
            id = mym(['select job from ', obj.db_table, ' ', ...
                'order by job desc limit 1']);
            
            job = id.job;
            
            if job % if no job entry exist
                task_id = job + 1;
            else
                task_id = 1;
            end
            
            % if task_id would exceed largest integer value
            if task_id >= 2^32
                obj.clear_table;
                task_id = 1;
            end
            
            mym('UNLOCK TABLES');
            
            mym('close');
            
        end
        
        function clear_table(obj)
            
            mym_connect(obj.db_server, obj.db_user, obj.db_passwd, obj.db_database);
            
            mym(['LOCK TABLES ',obj.db_table,' WRITE']);
            
            mym(['truncate table ', obj.db_table]);
            
            mym('UNLOCK TABLES');
            
            mym('close');
            
        end
        
        function progress(obj)
            % PROGRESS Shows a progress map
            %
            %   This function visualizes the progress of the computations on the grid.
            %       black       Not started WP
            %       yellow      Running WP
            %       red         Finished WP
            
            tic;
            no_wp = obj.wp_total;
            rc = ceil( sqrt(no_wp)-1e-5 );
            pp = 0;
            
            h = figure;
            isFigureHandle = ishandle(h) && strcmp(get(h,'type'),'figure');
            isInit = false; isTIC = true;
            while pp < 100 && isFigureHandle
                obj.update;
                if pp > 0 && isTIC; tic; isTIC = false; end
                
                stat = zeros( rc,rc );
                stat(no_wp+1:end) = 10;
                stat( obj.status == 1 ) = 7;
                stat( obj.status == 2 | obj.status == 3 ) = 4;
                
                if ~isInit
                    figure( h )
                    hi = imagesc( stat );
                    colormap( 'hot' )
                    caxis([0,10])
                    isInit = true;
                else
                    set(hi,'CData',stat);
                end
                
                pp = sum( obj.status == 2 | obj.status == 3 ) / obj.wp_total * 100;
                title( [num2str(pp,'%1.1f'),' %'] );
                drawnow
                pause(1)
                isFigureHandle = ishandle(h) && strcmp(get(h,'type'),'figure');
            end
            toc;
        end
    end
    
    
    methods(Static)
        function [obj,task] = create( no_wps, task_number, login_file_name )
            % CREATE Creates a new task
            %
            %   This function creates a new task. If you have write
            %   permissions on the database, but not used the scheduler
            %   before, a new scheduling table is created. By default, the
            %   table has the same name as your user name.
            %
            %   Input:
            %       no_wps              Number of workpackages
            %       task_number         Desired task number
            %       login_file_name     The path to the login-file
            %
            %   Output:
            %       obj          A new "mym_parallel" object
            %       task         The task-ID for the now object
            
            if exist('login_file_name','var')
                obj = mym_parallel( [], login_file_name );
            else
                obj = mym_parallel;
            end
            
            mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
            
            % Create table if it does not exist
            tables = mym('SHOW TABLES');
            tmp = fieldnames(tables);
                       
            if isempty( tables.(tmp{1}) ) || ~any( strcmp( tables.(tmp{1}), obj.db_table ) )
                mym(['CREATE TABLE ',obj.db_table,' (',...
                    'job INT UNSIGNED NOT NULL AUTO_INCREMENT, ',...
                    'task INT UNSIGNED NOT NULL DEFAULT 0, ',...
                    'wp INT UNSIGNED NOT NULL DEFAULT 0, ',...
                    'status TINYINT UNSIGNED NOT NULL DEFAULT 0, ',...
                    'block TINYINT UNSIGNED NOT NULL DEFAULT 0, ',...
                    'depend INT UNSIGNED NOT NULL DEFAULT 0, ',...
                    'PRIMARY KEY (job), ',...
                    'INDEX task (task ASC) ) ',...
                    'ENGINE = MyISAM;']);
            else
                columns = mym(['SHOW COLUMNS FROM ',obj.db_table]);
                if ~all( strcmp( columns.Field , 'job' ) + ...
                        strcmp( columns.Field , 'task' ) + ...
                        strcmp( columns.Field , 'wp' ) + ...
                        strcmp( columns.Field , 'status' ) + ...
                        strcmp( columns.Field , 'block' ) + ...
                        strcmp( columns.Field , 'depend' ) )
                    error(['Table "',obj.db_table,'" has wrong layout.'])
                end
            end
            
            if ~exist('task_number','var') || isempty(task_number)
                % Get new task ID
                task = mym(['select max(task) as ma from ',obj.db_table]);
                if isnan( task.ma )
                    task = 1;
                else
                    task = task.ma + 1;
                end
                mym('close')
            else
                job_tmp = mym_parallel( task_number, obj.login_file_name  );
                job_tmp.update;
                if job_tmp.wp_total ~= 0
                    warning(['Overwriting exisiting task ',num2str(task_number),'.'])
                    job_tmp.delete_task;
                end
                task = task_number;
            end
            
            
            % Enter new job into database
            list = sprintf('(%1.16g,%1.16g,%1.16g),' ,...
                [task(ones(1,no_wps)) ; 1:no_wps ; zeros(1,no_wps)] );
            
            mym_connect( obj.db_server, obj.db_user, obj.db_passwd, obj.db_database );
            mym(['INSERT INTO ',obj.db_table,'(task,wp,status) VALUES ' list(1:end-1) '' ]);
            mym('close')
            
            obj.task = task;
            obj.wp_total = no_wps;
            obj.wp_todo = no_wps;
            obj.wp_running = 0;
            obj.wp_finished = 0;
        end
        
        function fn = login_file
            fn = which('mym_parallel_login.txt');
            if isempty( fn )
                error('The login file "mym_parallel_login.txt" must be on your MATLAB search path.')
            end
        end
        
        function login_data = parse_login_file( fn )
            if ~exist( 'fn','var' ) || isempty( fn )
                fn = mym_parallel.login_file;
            else
                tmp = dir( fn );
                if numel(tmp) ~= 1
                    error('Login-file not found.')
                end
            end
            
            login_data = struct( ...
                'db_server'     , '' , ...
                'db_database'   , '' , ...
                'db_user'       , '' , ...
                'db_passwd'     , '' , ...
                'db_table'      , '' );
            
            % The allowed field names
            names = fieldnames(login_data);
            
            % Read file line by line and parse the data
            file = fopen(fn,'r');
            lin = fgetl(file);
            while ischar(lin)
                
                % Check if there is an equal sign
                p1 = regexp(lin,'=');
                if ~isempty(p1)
                    
                    % Check if the line is commented
                    p2 = regexp(lin(1:p1(1)-1),'%', 'once');
                    if isempty(p2)
                        
                        % Read name of the variable
                        name = regexp( lin(1:p1(1)-1) ,'[A-Za-z0-9_]+','match');
                        if ~isempty(name)                   % If there is a name
                            
                            % Get the index in the logindata-struct
                            ind = strcmp(name,names);
                            
                            if any(ind)  % If the index exists
                                
                                % Read value
                                p3 = regexp( lin(p1(1)+1:end)  ,'[A-Za-z0-9_\-.]+','match');
                                if numel( p3 )==1
                                    login_data.( names{ind} ) = p3{1};
                                end
                            end
                        end
                    end
                end
                lin = fgetl(file);
            end
            fclose(file);
        end
    end
end

