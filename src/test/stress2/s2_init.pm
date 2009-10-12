# Define our connections, we use DBD::Pg as well as system calls


sub master_connect {
    $m0_dbh = DBI->connect("dbi:Pg:dbname=bench user=$user port=5500 password='foo'");
    }
   
sub slave0_connect {
    $s0_dbh = DBI->connect("dbi:Pg:dbname=bench user=$user port=5501 password='foo'");
    }

sub slave1_connect {
    $s1_dbh = DBI->connect("dbi:Pg:dbname=bench user=$user port=5502 password='foo'");
    }

sub all_connect {
    &master_connect();
    &slave0_connect();
    &slave1_connect();
    }

sub all_close {
    $m0_dbh->disconnect;    
    $s0_dbh->disconnect;    
    $s1_dbh->disconnect;    
    }
   

#
# Reminder, the test requires that the bench database exist and be replicated before running
#

sub drop_tables {
    print "\n\nThis may fail\n\n";
    &all_connect();
    $sql = "ALTER TABLE account DISABLE REPLICATION; ALTER TABLE branch DISABLE REPLICATION; ALTER TABLE teller DISABLE REPLICATION;ALTER TABLE insert_test DISABLE REPLICATION; ALTER TABLE three_column_text DISABLE REPLICATION;";
    $m0_dbh->do("$sql");        
    $sql = "DROP TABLE account CASCADE; DROP TABLE branch CASCADE; DROP TABLE teller CASCADE; DROP TABLE insert_test CASCADE; DROP TABLE three_column_text CASCADE;";
    $m0_dbh->do("$sql");
    $s0_dbh->do("$sql");
    $s1_dbh->do("$sql");
    
    &all_close();

    }
    
sub init_tables {
    &all_connect();
       
    #
    # Table for two_column_text
    #
    print "Init Table: three_column text\n\n";
    $sql = "CREATE TABLE three_column_text (id bigserial primary key, test1 text, test2 text)";   
    $m0_dbh->do("$sql");
    $s0_dbh->do("$sql");
    $s1_dbh->do("$sql");
    
    #
    # Table for massive insert test
    #
    print "Init Table: insert_test\n\n";
    $sql = "CREATE TABLE insert_test (id bigserial primary key, test1 text)";
    $m0_dbh->do("$sql");
    $s0_dbh->do("$sql");
    $s1_dbh->do("$sql");
    
    #
    # Initialize odbc-bench
    #
    print "Init obench tables\n\n";
    system("/usr/local/bin/odbc-bench-cmd -d master0 -u $user -C");
    system("/usr/local/bin/odbc-bench-cmd -d slave0 -u $user -C");
    system("/usr/local/bin/odbc-bench-cmd -d slave1 -u $user -C");
    
    #
    # Replicator requires primary keys, so we drop the unique indexes and add primary keys
    #

    $m0_dbh->do("DROP INDEX accountix");
    $s0_dbh->do("DROP INDEX accountix");
    $s1_dbh->do("DROP INDEX accountix");
    
    $m0_dbh->do("ALTER TABLE account ADD PRIMARY KEY (account)");
    $s0_dbh->do("ALTER TABLE account ADD PRIMARY KEY (account)");
    $s1_dbh->do("ALTER TABLE account ADD PRIMARY KEY (account)");
    
    $m0_dbh->do("DROP INDEX branchix");
    $s0_dbh->do("DROP INDEX branchix");
    $s1_dbh->do("DROP INDEX branchix");
    
    $m0_dbh->do("ALTER TABLE branch ADD PRIMARY KEY (branch)");
    $s0_dbh->do("ALTER TABLE branch ADD PRIMARY KEY (branch)");
    $s1_dbh->do("ALTER TABLE branch ADD PRIMARY KEY (branch)");
    
    $m0_dbh->do("DROP INDEX tellerix");
    $s0_dbh->do("DROP INDEX tellerix");
    $s1_dbh->do("DROP INDEX tellerix");
    
    $m0_dbh->do("ALTER TABLE teller ADD PRIMARY KEY (teller)");
    $s0_dbh->do("ALTER TABLE teller ADD PRIMARY KEY (teller)");
    $s1_dbh->do("ALTER TABLE teller ADD PRIMARY KEY (teller)");
    
    # Close connections
    
    &all_close();
    
    }
    

sub init_replication {
    &all_connect();
    $m0_dbh->begin_work;
    $m0_dbh->do("ALTER TABLE account ENABLE REPLICATION");
    $m0_dbh->do("ALTER TABLE branch ENABLE REPLICATION");
    $m0_dbh->do("ALTER TABLE teller ENABLE REPLICATION");
    $m0_dbh->do("ALTER TABLE insert_test ENABLE REPLICATION");
    $m0_dbh->do("ALTER TABLE three_column_text ENABLE REPLICATION");
        
    # Declare which tables get replicated to which slave
    
    $m0_dbh->do("ALTER TABLE account ENABLE REPLICATION ON SLAVE 0");
    $m0_dbh->do("ALTER TABLE branch ENABLE REPLICATION ON SLAVE 0");
    $m0_dbh->do("ALTER TABLE teller ENABLE REPLICATION ON SLAVE 0");
    $m0_dbh->do("ALTER TABLE insert_test ENABLE REPLICATION ON SLAVE 0");
    $m0_dbh->do("ALTER TABLE three_column_text ENABLE REPLICATION ON SLAVE 0");
    
    $m0_dbh->do("ALTER TABLE account ENABLE REPLICATION ON SLAVE 1");
    $m0_dbh->do("ALTER TABLE branch ENABLE REPLICATION ON SLAVE 1");
    # We purposely do not replicate table teller to slave 1
    $m0_dbh->do("ALTER TABLE insert_test ENABLE REPLICATION ON SLAVE 1");
    $m0_dbh->do("ALTER TABLE three_column_text ENABLE REPLICATION ON SLAVE 1");
    
    $m0_dbh->commit;
    
    &all_close();
    }

1;