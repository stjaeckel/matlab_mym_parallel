function mym_connect( db_server, db_user, db_passwd, db_database )
% MYM_CONNECT Connects to a MYSQL database

not_connected = true;

test = mym('status');
if test == 0
    not_connected = false;
end

while not_connected
    try
        a = mym('open', db_server, db_user, db_passwd,'false');
        b = mym('use',db_database);
        not_connected = false;
    catch err
        disp(err.message)
        disp('Waiting for 30 seconds ...')
        pause(30)
    end
end

end
