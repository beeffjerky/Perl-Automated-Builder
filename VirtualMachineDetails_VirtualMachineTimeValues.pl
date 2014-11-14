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
                    
use constant TEST_QUERY         =>  'exec Savision_CloudReporter_Report_HyperV_VirtualMachineDetails_TimeValues
                                    @VMId = ?,
                                    @NumberOfDaysOfHistoricalData = ?'
;
use constant DSN =>'dbi:ODBC:CloudReporterTest';

my $connection = DBIx::Connection->new(name => 'test', dsn => DSN, username => 'HEROES\mike', password => 'P@ssw0rd');
my $g_dbh1 = DBI->connect(DSN);

EmptyTables();
NonRecentDailyData(2,2,1);
UnclusteredVMWithRelevantData(2,2);
ClusteredVMWithRelevantData(2,1,2);
DataBeforeAndAfterSpecifiedThreshold();
UnclusteredVMWithRelevantData();
MultipleDataEntriesPerDay ();
VMNoEnteredData();
MissingDataEntry();
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
                                            
                                                VMs =>  [
                                                            "Loop_VM1",
                                                            {
                                                                DailyData=> [
                                                                                {
                                                                                    RuleId =>30,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-11)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>36,
                                                                                    Percentile_95=>20480000,
                                                                                    Percentile_50=>10240000,
                                                                                    DateTime=>SqlDate(-11)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>40,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-11)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-11)                                                                                                                          
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    LastValue=> 2048,
                                                                                    DateTime=>SqlDate(-11)                                                                                                                          
                                                                                },
                                                                            ]
                                                            },
                                                            "End_Loop"
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
                Loop_VM1        =>shift @_
                );

    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        my $rows = 0;
        while( $sth->fetchrow_arrayref) {
            $rows++;
        }
        is ($rows,0,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH DATA OUT OF DATE BY ONE DAY");
    }
}

sub ClusteredVMWithRelevantData
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [
                                                            "Loop_VM1",
                                                            {
                                                                DailyData=> [
                                                                                {
                                                                                    RuleId =>30,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-5)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>36,
                                                                                    Percentile_95=> 2097152,
                                                                                    Percentile_50=> 2097152/2,
                                                                                    DateTime=>SqlDate(-5)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>40,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-5)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-5)                                                                                                                          
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    LastValue=> 2048,
                                                                                    DateTime=>SqlDate(-5)                                                                                                                          
                                                                                },
                                                                            ]
                                                            },
                                                            "End_Loop"
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
        Loop_VM1        =>shift @_
        );
    EnvironmentBuild (\%Env,\%loop);

    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1} CLUSTERED VMS WITH FILLED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[1],"1048.0","TESTING P95_DynamicMemeoryRequiredInMB CLUSTERED VM WITH FILLED DAILY DATA");
        is (@{$row}[2],"2048.0","TESTING P50_DynamicMemeoryRequiredInMB CLUSTERED VM WITH FILLED DAILY DATA");
        is (@{$row}[3],"2.0","TESTING P95_NetworkReadsAndWritesInMBps CLUSTERED VM WITH FILLED DAILY DATA");
        is (@{$row}[4],"1.0","TESTING P50_NetworkReadsAndWritesInMBps CLUSTERED VM WITH FILLED DAILY DATA");
        is (@{$row}[5],"1048.0","TESTING P95_ProcessorLoadOnHostInMhz CLUSTERED VM WITH FILLED DAILY DATA");
        is (@{$row}[6],"2048.0","TESTING P50_ProcessorLoadOnHostInMhz CLUSTERED VM WITH FILLED DAILY DATA");
        is (@{$row}[7],"1048.0","TESTING P95_UsedPhysicalRamInMB CLUSTERED VM WITH FILLED DAILY DATA");
        is (@{$row}[8],"2048.0","TESTING P50_UsedPhysicalRamInMB CLUSTERED VM WITH FILLED DAILY DATA");
        is (@{$row}[9],"2.0","TESTING LV_UsedVhdDiskSpaceInGB CLUSTERED VM WITH FILLED DAILY DATA");
    }
}

sub UnclusteredVMWithRelevantData
{
    my %Env = (                       
        Hosts =>    [   
                        "Loop_Host1",
                        {     
                            VMs =>  [
                                        "Loop_VM1",
                                        {
                                            DailyData=> [
                                                            {
                                                                RuleId =>30,
                                                                Percentile_95=> 1048,
                                                                Percentile_50 => 2048,
                                                                DateTime=>SqlDate(-5)                                                               
                                                            },
                                                            {
                                                                RuleId =>36,
                                                                Percentile_95=> 2097152,
                                                                Percentile_50=> 2097152/2,
                                                                DateTime=>SqlDate(-5)                                                               
                                                            },
                                                            {
                                                                RuleId =>40,
                                                                Percentile_95=> 1048,
                                                                Percentile_50 => 2048,
                                                                DateTime=>SqlDate(-5)                                                               
                                                            },
                                                            {
                                                                RuleId =>42,
                                                                Percentile_95=> 1048,
                                                                Percentile_50 => 2048,
                                                                DateTime=>SqlDate(-5)                                                                                                                          
                                                            },
                                                            {
                                                                RuleId =>1,
                                                                LastValue=> 2048,
                                                                DateTime=>SqlDate(-5)                                                                                                                          
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
        Loop_Host1      =>shift @_,
        Loop_VM1        =>shift @_
        );
    EnvironmentBuild (\%Env,\%loop);

    for( my $number = 1; $number != $loop{Loop_Host1}*$loop{Loop_VM1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED VMS WITH FILLED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[1],"1048.0","TESTING P95_DynamicMemeoryRequiredInMB UNCLUSTERED VMS WITH FILLED DAILY DATA");
        is (@{$row}[2],"2048.0","TESTING P50_DynamicMemeoryRequiredInMB UNCLUSTERED VMS WITH FILLED DAILY DATA");
        is (@{$row}[3],"2.0","TESTING P95_NetworkReadsAndWritesInMBps UNCLUSTERED VMS WITH FILLED DAILY DATA");
        is (@{$row}[4],"1.0","TESTING P50_NetworkReadsAndWritesInMBps UNCLUSTERED VMS WITH FILLED DAILY DATA");
        is (@{$row}[5],"1048.0","TESTING P95_ProcessorLoadOnHostInMhz UNCLUSTERED VMS WITH FILLED DAILY DATA");
        is (@{$row}[6],"2048.0","TESTING P50_ProcessorLoadOnHostInMhz UNCLUSTERED VMS WITH FILLED DAILY DATA");
        is (@{$row}[7],"1048.0","TESTING P95_UsedPhysicalRamInMB UNCLUSTERED VMS WITH FILLED DAILY DATA");
        is (@{$row}[8],"2048.0","TESTING P50_UsedPhysicalRamInMB UNCLUSTERED VMS WITH FILLED DAILY DATA");
        is (@{$row}[9],"2.0","TESTING LV_UsedVhdDiskSpaceInGB UNCLUSTERED VMS WITH FILLED DAILY DATA");
    }
}

sub DataBeforeAndAfterSpecifiedThreshold
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {   
                                                VMs =>  [
                                                            "Loop_VM1",
                                                            {                                            
                                                                DailyData=> [
                                                                                {
                                                                                    RuleId =>30,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,          
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>36,
                                                                                    Percentile_95=> 2097152,
                                                                                    Percentile_50=> 2097152/2,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }                                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>40,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }                                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    LastValue=> 2048,
                                                                                    DateTime=>SqlDate(-5)  , 
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
                        },
                        "End_Loop"
                    ]
    );        
    my %loop = (
            Loop_Cluster1   =>1,
            Loop_Host1      =>1,
            Loop_VM1        =>1
        );
    EnvironmentBuild (\%Env,\%loop);
    my $sth = $g_dbh1 -> prepare(TEST_QUERY);
    $sth->execute(VMIdFromEntity(1),1);
    my $rows = 0;
    my $row;
    while( $row = $sth->fetchrow_arrayref) {
        $rows++;
    }
    
    is ($rows,1,"TESTING 1 ROW AFTER THRESHOLD ");
    $sth = $g_dbh1 -> prepare( TEST_QUERY);
    $sth->execute(VMIdFromEntity(1),3);
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
                                                VMs =>  [
                                                            {                                            
                                                                DailyData=> [
                                                                                {
                                                                                    RuleId =>30,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,          
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>36,
                                                                                    Percentile_95=> 2097152,
                                                                                    Percentile_50=> 2097152/2,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }                                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>40,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }                                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    LastValue=> 2048,
                                                                                    DateTime=>SqlDate(-5)  , 
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-1,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>30,
                                                                                    Percentile_95=> 5678,
                                                                                    Percentile_50=> 1234,          
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-2,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>36,
                                                                                    Percentile_95=> 2097152*2,
                                                                                    Percentile_50=> 2097152,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-2,
                                                                                                    OffSet=>2
                                                                                                }                                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>40,
                                                                                    Percentile_95=> 5678,
                                                                                    Percentile_50 =>1234,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-2,
                                                                                                    OffSet=>2
                                                                                                }                                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    Percentile_95=> 5678,
                                                                                    Percentile_50 =>1234,
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-2,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    LastValue=> 2048*2,
                                                                                    DateTime=>SqlDate(-5)  , 
                                                                                    History=>   {
                                                                                                    StartDate=>4,
                                                                                                    OffSetHours=>-2,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                }                        
                                                                                
                                                                            ]
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
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,6,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED VMs WITH MULTIPLE DAILY DATA");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        while( $row = $sth->fetchrow_arrayref) {
            if (@{$row}[0] eq SqlDate(-2,-2)){
                is (@{$row}[1],"5678.0","TESTING P95_DynamicMemeoryRequiredInMB CLUSTERED VM WITH MULTIPLE DAILY DATA");
                is (@{$row}[2],"1234.0","TESTING P50_DynamicMemeoryRequiredInMB CLUSTERED VM WITH MULTIPLE DAILY DATA");
                is (@{$row}[3],"4.0","TESTING P95_NetworkReadsAndWritesInMBps CLUSTERED VM WITH MULTIPLE DAILY DATA");
                is (@{$row}[4],"2.0","TESTING P50_NetworkReadsAndWritesInMBps CLUSTERED VM WITH MULTIPLE DAILY DATA");
                is (@{$row}[5],"5678.0","TESTING P95_ProcessorLoadOnHostInMhz CLUSTERED VM WITH MULTIPLE DAILY DATA");
                is (@{$row}[6],"1234.0","TESTING P50_ProcessorLoadOnHostInMhz CLUSTERED VM WITH MULTIPLE DAILY DATA");
                is (@{$row}[7],"5678.0","TESTING P95_UsedPhysicalRamInMB CLUSTERED VM WITH MULTIPLE DAILY DATA");
                is (@{$row}[8],"1234.0","TESTING P50_UsedPhysicalRamInMB CLUSTERED VM WITH MULTIPLE DAILY DATA");
                is (@{$row}[9],"4.0","TESTING LV_UsedVhdDiskSpaceInGB CLUSTERED VM WITH MULTIPLE DAILY DATA");
            }
        }
        

    }


}

sub VMNoEnteredData
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [
                                                            {
                                                                DailyData=> [
                                                                                {
                                                                                    RuleId =>30,
                                                                                    DateTime=>SqlDate(-5)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>36,
                                                                                    DateTime=>SqlDate(-5)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>40,
                                                                                    DateTime=>SqlDate(-5)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    DateTime=>SqlDate(-5)                                                                                                                          
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    DateTime=>SqlDate(-5)                                                                                                                          
                                                                                },
                                                                            ]
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
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED VM WITH UNPOPULATED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[1],undef,"TESTING P95_DynamicMemeoryRequiredInMB CLUSTERED VM WITH UNPOPULATED DATA");
        is (@{$row}[2],undef,"TESTING P50_DynamicMemeoryRequiredInMB CLUSTERED VM WITH UNPOPULATED DATA");
        is (@{$row}[3],undef,"TESTING P95_NetworkReadsAndWritesInMBps CLUSTERED VM WITH UNPOPULATED DATA");
        is (@{$row}[4],undef,"TESTING P50_NetworkReadsAndWritesInMBps CLUSTERED VM WITH UNPOPULATED DATA");
        is (@{$row}[5],undef,"TESTING P95_ProcessorLoadOnHostInMhz CLUSTERED VM WITH UNPOPULATED DATA");
        is (@{$row}[6],undef,"TESTING P50_ProcessorLoadOnHostInMhz CLUSTERED VM WITH UNPOPULATED DATA");
        is (@{$row}[7],undef,"TESTING P95_UsedPhysicalRamInMB CLUSTERED VM WITH UNPOPULATED DATA");
        is (@{$row}[8],undef,"TESTING P50_UsedPhysicalRamInMB CLUSTERED VM WITH UNPOPULATED DATA");
        is (@{$row}[9],undef,"TESTING LV_UsedVhdDiskSpaceInGB CLUSTERED VM WITH UNPOPULATED DATA");
    }
}

sub MissingDataEntry
{
     my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [
                                                            {
                                                                DailyData=> [
                                                                                {
                                                                                    RuleId =>30,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-5)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>40,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-5)                                                               
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    Percentile_95=> 1048,
                                                                                    Percentile_50 => 2048,
                                                                                    DateTime=>SqlDate(-5)                                                                                                                          
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    LastValue=> 2048,
                                                                                    DateTime=>SqlDate(-5)                                                                                                                          
                                                                                },
                                                                            ]
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
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED VM WITH MISSING DAILY DATA");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(VMIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[3],undef,"TESTING P95_NetworkReadsAndWritesInMBps from NetworkReadandWritesInBPS MISSING FROM HOSTS WITH FILLED DAILY DATA");
        is (@{$row}[4],undef,"TESTING P50_NetworkReadsAndWritesInMBps from NetworkReadandWritesInBPS MISSING FROM HOSTS WITH FILLED DAILY DATA");
    }
}

sub VMIdFromEntity
{
    my $managedId = shift @_;
    my $sth = $g_dbh1->prepare ("Select VMId from SavisionCloudReporter.VMManagedEntityMap where ManagedEntityRowId = $managedId");
    $sth->execute();
    return $sth->fetchrow_array;
}





