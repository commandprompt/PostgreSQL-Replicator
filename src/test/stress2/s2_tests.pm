# Define your tests here
    
sub truncate {
   &master_connect();
   $query = $m0_dbh->do("TRUNCATE insert_test");
   $query = $m0_dbh->do("TRUNCATE three_column_text");
   $m0_dbh->disconnect;
   }
   
sub obench_async {
   print "ODBCng Buffering Async\n";
   print "==========================================================\n";
   system("/usr/local/bin/odbc-bench-cmd -d \"master0\" -u $user -r 1 -t 10 -s exec -c keyset -S 10 -K 10 -v -a");
   }
   
sub obench_sync {
   print "ODBCng Buffering\n";
   print "==========================================================\n";
   system("/usr/local/bin/odbc-bench-cmd -d \"master0\" -u $user -r 1 -t 10 -s exec -c keyset -S 10 -K 10 -v ");
   }   
   
sub insert {
   &master_connect();
   print "Start: " . `date +%X` . " ";
   print "\n" . "Insert 50000 rows one at a time" . " ";
   $sql = "INSERT INTO insert_test(test1) values ('HELLO')";
   for my $i (1..50000) { $query = $m0_dbh->do("$sql"); }
   print "\n" . "End: " . `date +%X` . "\n";
   # Disconnect when finished.
   $m0_dbh->disconnect;
   }
   
sub insert_bulk {
   &master_connect();
   print "Start: " . `date +%X` . " ";
   print "\n" . "Insert 50000 rows 1000 at a time" . " ";
   for my $i (1..100) {
      $m0_dbh->begin_work;
      $sql = "INSERT INTO insert_test(test1) values ('HELLO')";
      for my $i (1..500) { $query = $m0_dbh->do("$sql"); 
      }
     
      $m0_dbh->commit;
   }
   print "\n" . "End: " . `date +%X` . "\n";
   # Disconnect when finished.
   $m0_dbh->disconnect;
   }   
   
sub insert_prepare {
   &master_connect();
   print "Start: " . `date +%X` . " ";
   print "\n" . "Insert 50000 prepared rows 500 at a time" . " ";
   for my $i (1..100) {
      $m0_dbh->begin_work;
      $sth = $m0_dbh->prepare("INSERT INTO three_column_text(test1,test2) values (?,?)");             
      
      for my $i (1..500) { 
         $sth->execute("Hello","Goodbye");            
      }
     
      $m0_dbh->commit;
   }
   print "\n" . "End: " . `date +%X` . "\n";
   # Disconnect when finished.
   $m0_dbh->disconnect;
   }   

1;