use strict;
use File::path;
use DBI;
use DBD::ODBC;
use DBIx::Connection;
use DBIx::PLSQLHandler;
use DBUnit ':all';
use DateTime;
use DateTime::Duration;
use Test::DBUnit dsn => 'dbi:ODBC:CloudReporterTest', username => '', password => '';
use Test::More;
use Constant;
use Class::Struct;
use POSIX ();
use POSIX qw(strftime);
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/Perl SQL Testing/lib';
use CloudReporter::Build qw(Reset);
use CloudReporter::Environment qw(EnvironmentBuild SqlDate);
                    
use constant TEST_QUERY         =>  'exec Savision_CloudReporter_Report_HyperV_HostDetails_HostTimeValues
                                    @HostId = ?,
                                    @NumberOfDaysOfHistoricalData = ?'
;
use constant DSN =>'dbi:ODBC:CloudReporterTest';

my $connection = DBIx::Connection->new(name => 'test', dsn => DSN, username => 'HEROES\mike', password => 'P@ssw0rd');
my $g_dbh1 = DBI->connect(DSN);


EmptyTables();
NonRecentDailyData(2,2);
HostsWithRelevantData(2,2);
HostsNoEnteredData(2,2);
HostsWithMissingData(2,2);
HostsWithRelevantData(1,1);
MultipleDataEntriesPerDay(1,1);
DataBeforeAndAfterThreshold();
done_testing();

sub EmptyTables
{
    Reset("VM","Host","ClusterStorage","Cluster");
    my $value = 1;

    my $sth = $g_dbh1->prepare ('Select top 1 ("HostId") from SavisionCloudReporter.Host ORDER BY "HostId" DESC');
    $sth->execute;

    if ($sth->fetchrow_arrayref){
        $value = @{$_}[0];
    }
    $sth = undef;
    $sth = $g_dbh1 -> prepare( TEST_QUERY);
    $sth->execute($value,9);
    my $rows = 0;
    my $row;
    while( $row = $sth->fetchrow_arrayref) {
        $rows++;
    }

    is ($rows,0,"TESTING EMPTY TABLE");
}

sub NonRecentDailyData
{
    my %Env = (
        Clusters =>  [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [
                                            "Loop_Host1",
                                            {
                                                InstalledMemoryMB => 12754,
                                                DailyData       =>      [
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-11)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-11)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-11)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-11)                                                                                                                          
                                                                            },
                                                                        ]
                                            },
                                            "End_Loop"
                                        ]
                        },
                        "End_Loop"
                    ]
    );
    my %loop = (
                Loop_Cluster1   =>shift @_,
                Loop_Host1      =>shift @_,
                );

    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        while( $sth->fetchrow_arrayref) {
            $rows++;
        }
        is ($rows,0,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH DATA OUT OF DATE BY ONE DAY");
    }
}

sub HostsWithRelevantData
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                InstalledMemoryMB => 12754,
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                            }
                                                                            
                                                                        ]
                                            },
                                            "End_Loop"
                                        ]
                        },
                        "End_Loop"
                    ]
    );        
    my %loop = (
        Loop_Cluster1   =>shift @_,
        Loop_Host1      =>shift @_,
        );
    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,4,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH FILLED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[1],"1.0","TESTING P5_AvailablePhysicalRamInGB CLUSTERED HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[2],"2.0","TESTING P50_AvailablePhysicalRamInGB CLUSTERED HOSTS WITH FILLED DAILY DATA");
        is (@{$row}[3],"2.0","TESTING NumberOfRunningVirtualMachines CLUSTERED HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[4],"2.0","TESTING AvailableDiskSpaceInGB CLUSTERED HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[5],"1024.0","TESTING P5_TotalProcessorIdleMhz CLUSTERED HOSTS WITH FILLED DAILY DATA");
        is (@{$row}[6],"2048.0","TESTING P5_TotalProcessorIdleMhz CLUSTERED HOSTS WITH FILLED DAILY DATA");
    }

}

sub HostsWithRelevantData
{
    my %Env = (
        Hosts =>    [   
                        "Loop_Host1",
                        {     
                            InstalledMemoryMB => 12754,
                            DailyData           =>  [
                                                        {
                                                            RuleId =>9,
                                                            LastValue=> 2048,
                                                            Percentile_5=> 1024,
                                                            Percentile_50 => 2048,
                                                            History=>   {
                                                                            StartDate=>9,
                                                                            OffSet=>3
                                                                        }
                                                           
                                                        },
                                                        {
                                                            RuleId =>17,
                                                            LastValue=> 2,
                                                            Percentile_5=>0,
                                                            Percentile_50=>3,
                                                            History=>   {
                                                                            StartDate=>9,
                                                                            OffSet=>3
                                                                        }
                                                           
                                                        },
                                                        {
                                                            RuleId =>8,
                                                            LastValue=> 2048,
                                                            Percentile_5=> 1024,
                                                            Percentile_50 => 2048,
                                                            History=>   {
                                                                            StartDate=>9,
                                                                            OffSet=>3
                                                                        }
                                                           
                                                        },
                                                        {
                                                            RuleId =>21,
                                                            LastValue=> 2048,
                                                            Percentile_5=> 1024,
                                                            Percentile_50 => 2048,
                                                            History=>   {
                                                                            StartDate=>9,
                                                                            OffSet=>3
                                                                        }
                                                        }
                                                        
                                                    ]
                        },
                        "End_Loop"
                    ]       
    );        
    my %loop = (
        Loop_Cluster1   =>shift @_,
        Loop_Host1      =>shift @_,
        );
    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,4,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS WITH FILLED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[1],"1.0","TESTING P5_AvailablePhysicalRamInGB UNCLUSTERED HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[2],"2.0","TESTING P50_AvailablePhysicalRamInGB UNCLUSTERED HOSTS WITH FILLED DAILY DATA");
        is (@{$row}[3],"2.0","TESTING NumberOfRunningVirtualMachines UNCLUSTERED HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[4],"2.0","TESTING AvailableDiskSpaceInGB UNCLUSTERED HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[5],"1024.0","TESTING P5_TotalProcessorIdleMhz UNCLUSTERED HOSTS WITH FILLED DAILY DATA");
        is (@{$row}[6],"2048.0","TESTING P5_TotalProcessorIdleMhz UNCLUSTERED HOSTS WITH FILLED DAILY DATA");
    }

}

sub DataBeforeAndAfterThreshold
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>4,
                                                                                                OffSetHours=>-1,
                                                                                                OffSet=>2
                                                                                            }
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                History=>   {
                                                                                                StartDate=>4,
                                                                                                OffSetHours=>-1,
                                                                                                OffSet=>2
                                                                                            }                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>4,
                                                                                                OffSetHours=>-1,
                                                                                                OffSet=>2
                                                                                            }                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>4,
                                                                                                OffSetHours=>-1,
                                                                                                OffSet=>2
                                                                                            }
                                                                            }                                                                            
                                                                        ]
                                            },
                                            "End_Loop"
                                        ]
                        },
                        "End_Loop"
                    ]
    );        
    my %loop = (
        Loop_Cluster1   =>1,
        Loop_Host1      =>1,
        );
    EnvironmentBuild (\%Env,\%loop);
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity(1),1);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING 1 ROW AFTER THRESHOLD ");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity(1),3);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        is ($rows,2,"TESTING 2 ROWS AFTER THRESHOLD ");
        


}

sub MultipleDataEntriesPerDay
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                InstalledMemoryMB => 12754,
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                            },
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 1048,
                                                                                Percentile_5=> 1524,
                                                                                Percentile_50 => 2448,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSetHours => -2,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 9,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSetHours => -2,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                LastValue=> 1238,
                                                                                Percentile_5=> 1224,
                                                                                Percentile_50 => 2258,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSetHours => -2,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2348,
                                                                                Percentile_5=> 1124,
                                                                                Percentile_50 => 3548,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSetHours => -2,
                                                                                                OffSet=>3
                                                                                            }
                                                                            }
                                                                            
                                                                        ]
                                            },
                                            "End_Loop"
                                        ]
                        },
                        "End_Loop"
                    ]
    );        
    my %loop = (
        Loop_Cluster1   =>shift @_,
        Loop_Host1      =>shift @_,
        );
    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,8,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH MULTIPLE DAILY DATA");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        while( $row = $sth->fetchrow_arrayref) {
            if (@{$row}[0] eq SqlDate(-3,-2)){
                is (@{$row}[1], 1524/1024, "TESTING P5_AvailablePhysicalRamInGB CLUSTERED HOSTS WITH MULTIPLE DAILY DATA");
                is (@{$row}[2],2448/1024,"TESTING P50_AvailablePhysicalRamInGB CLUSTERED HOSTS WITH MULTIPLE DAILY DATA");
                is (@{$row}[3],"9.0", "TESTING NumberOfRunningVirtualMachines CLUSTERED HOSTS WITH MULTIPLE DAILY DATA");
                is (@{$row}[4],1238/1024,"TESTING AvailableDiskSpaceInGB CLUSTERED HOSTS WITH MULTIPLE DAILY DATA");
                is (@{$row}[5],"1124.0","TESTING P5_TotalProcessorIdleMhz CLUSTERED HOSTS WITH MULTIPLE DAILY DATA");
                is (@{$row}[6],"3548.0","TESTING P5_TotalProcessorIdleMhz CLUSTERED HOSTS WITH MULTIPLE DAILY DATA");
            }
        }
        

    }


}

sub HostsNoEnteredData
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                InstalledMemoryMB => 12754,
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>9,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                History=>   {
                                                                                                StartDate=>9,
                                                                                                OffSet=>3
                                                                                            }
                                                                            }
                                                                        ]
                                            },
                                            "End_Loop"
                                        ]
                        },
                        "End_Loop"
                    ]
    );        
    my %loop = (
        Loop_Cluster1   =>shift @_,
        Loop_Host1      =>shift @_,
        );
    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,4,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH UNPOPULATED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[1],undef,"TESTING P5_AvailablePhysicalRamInGB CLUSTERED HOSTS WITH UNPOPULATED DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[2],undef,"TESTING P50_AvailablePhysicalRamInGB CLUSTERED HOSTS WITH UNPOPULATED DATA");
        is (@{$row}[3],undef,"TESTING NumberOfRunningVirtualMachines CLUSTERED HOSTS WITH UNPOPULATED DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[4],undef,"TESTING AvailableDiskSpaceInGB CLUSTERED HOSTS WITH UNPOPULATED DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[5],undef,"TESTING P5_TotalProcessorIdleMhz CLUSTERED HOSTS WITH UNPOPULATED DATA");
        is (@{$row}[6],undef,"TESTING P5_TotalProcessorIdleMhz CLUSTERED HOSTS WITH UNPOPULATED DATA");
    }
}

sub HostsWithMissingData
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                InstalledMemoryMB => 12754,
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>4,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                History=>   {
                                                                                                StartDate=>4,
                                                                                                OffSet=>3
                                                                                            }
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                History=>   {
                                                                                                StartDate=>4,
                                                                                                OffSet=>2
                                                                                            }
                                                                            }
                                                                        ]
                                            },
                                            "End_Loop"
                                        ]
                        },
                        "End_Loop"
                    ]
    );        
    my %loop = (
        Loop_Cluster1   =>shift @_,
        Loop_Host1      =>shift @_,
        );
    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,4,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH MISSING DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[4],undef,"TESTING AvailableDiskSpaceInGB MISSING FROM HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[4],undef,"TESTING AvailableDiskSpaceInGB MISSING FROM HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[4],undef,"TESTING AvailableDiskSpaceInGB MISSING FROM HOSTS WITH FILLED DAILY DATA");
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[4],undef,"TESTING AvailableDiskSpaceInGB MISSING FROM HOSTS WITH FILLED DAILY DATA");
    }
}

sub HostIdFromEntity
{
    my $managedId = shift @_;
    my $sth = $g_dbh1->prepare ("Select HostId from SavisionCloudReporter.HostManagedEntityMap where ManagedEntityRowId = $managedId");
    $sth->execute();
    return $sth->fetchrow_array;
}





