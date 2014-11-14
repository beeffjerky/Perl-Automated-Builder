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
                    
use constant TEST_QUERY         =>  'exec Savision_CloudReporter_Report_HyperV_HostDetails_VirtualMachines
                                    @HostId = ?,
                                    @NumberOfDaysOfHistoricalData = ?'
;
use constant DSN =>'dbi:ODBC:CloudReporterTest';

my $connection = DBIx::Connection->new(name => 'test', dsn => DSN, username => 'HEROES\mike', password => 'P@ssw0rd');
my $g_dbh1 = DBI->connect(DSN);

EmptyTables();
NonRecentDailyData(2,2);
RunningVMWithRelevantData(2,1,2);
PoweredOffVM(2,2);
VMHistoricalData(1,1);
MultipleDataEntriesPerDay(1,1,1);
VMNoEnteredData(1,1,1);
MissingDataEntry(1,1,1);
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

sub RunningVM's
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
                                                                ValidToDate => undef,
                                                                IsRunning =>1,
                                                                ValidFromDate => SqlDate(-9),
                                                                DataLastSeen => SqlDate(0),
                                                                DailyData=> [
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
    
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,$loop{Loop_VM1},"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH VM's");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[2],"1.0","TESTING OnHostPecentage VM Data CALCULATION");
        is (@{$row}[3],"1.0","TESTING RunningOnHostPecentage VM Data CALCULATION");
        is (@{$row}[4],"1048.0","TESTING p_95ProcessorLoadOnHost VM Data");
        is (@{$row}[5],"1048.0","TESTING p_95UsedPhysicalRamInMB VM Data CALCULATION");
        is (@{$row}[6],"2048.0","TESTING Max_UsedVhdDiskSpaceInMB VM Data CALCULATION");
    }
}

sub PoweredOffVM
{
    my %Env = (                       
        Hosts =>    [   
                        "Loop_Host1",
                        {     
                            VMs =>  [
                                        "Loop_VM1",
                                        {
                                            ValidToDate => undef,
                                            IsRunning =>0,
                                            ValidFromDate => SqlDate(-9),
                                            DataLastSeen => SqlDate(0),
                                            DailyData=> [
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

    for( my $number = 1; $number != $loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,$loop{Loop_VM1},"TESTING $number OUT OF $loop{Loop_Host1} HOSTS WITH OFF VM's");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[2],"1.0","TESTING OnHostPecentage OFF VM Data CALCULATION");
        is (@{$row}[3],"0.0","TESTING RunningOnHostPecentage OFF VM Data CALCULATION");
    }
}

sub VMHistoricalData
{
    my %Env = (                       
        Hosts =>    [   
                        "Loop_Host1",
                        {     
                            VMs =>  [
                                        "Loop_VM1",
                                        {
                                            ValidToDate => undef,
                                            IsRunning =>0,
                                            ValidFromDate => SqlDate(-9),
                                            DataLastSeen => SqlDate(0),
                                            DailyData=> [
                                                            {
                                                                RuleId =>40,
                                                                Percentile_95=> 1048,
                                                                Percentile_50 => 2048,
                                                                History     =>  {
                                                                                    StartDate=>9,
                                                                                    OffSet =>2
                                                                                }
                                                            },
                                                            {
                                                                RuleId =>42,
                                                                Percentile_95=> 1048,
                                                                Percentile_50 => 2048,
                                                                History     =>  {
                                                                                    StartDate=>9,
                                                                                    OffSet =>2
                                                                                }
                                                            },
                                                            {
                                                                RuleId =>1,
                                                                LastValue=> 2048,
                                                                History     =>  {
                                                                                    StartDate=>8,
                                                                                    OffSet =>2
                                                                                }
                                                            },
                                                            {
                                                                RuleId =>40,
                                                                Percentile_95=> 4096,
                                                                Percentile_50 => 2048,
                                                                History     =>  {
                                                                                    StartDate=>8,
                                                                                    OffSet =>2
                                                                                }
                                                            },
                                                            {
                                                                RuleId =>42,
                                                                Percentile_95=>4096,
                                                                Percentile_50 => 2048,
                                                                History     =>  {
                                                                                    StartDate=>8,
                                                                                    OffSet =>2
                                                                                }
                                                            },
                                                            {
                                                                RuleId =>1,
                                                                LastValue=> 4096,
                                                                History     =>  {
                                                                                    StartDate=>8,
                                                                                    OffSet =>2
                                                                                }
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
    for( my $number = 1; $number != $loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,$loop{Loop_VM1},"TESTING $number OUT OF $loop{Loop_Host1} HOSTS WITH OFF VM's");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[4],"2572.0","TESTING p_95ProcessorLoadOnHost VM Data CALCULATION");
        is (@{$row}[5],"2572.0","TESTING p_95UsedPhysicalRamInMB VM Data CALCULATION");
        is (@{$row}[6],"4096.0","TESTING Max_UsedVhdDiskSpaceInMB VM Data CALCULATION");
    }
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
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,
                                                                IsRunning =>1,
                                                                ValidFromDate => SqlDate(-9),
                                                                DataLastSeen => SqlDate(0),
                                                                DailyData=> [
                                                                                {
                                                                                    RuleId =>40,
                                                                                    Percentile_95=> 1024,
                                                                                    Percentile_50 => 2048,
                                                                                    History     =>  {
                                                                                                        StartDate=>8,
                                                                                                        OffSet =>2
                                                                                                    }
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    Percentile_95=> 1024,
                                                                                    Percentile_50 => 2048,
                                                                                    History     =>  {
                                                                                                        StartDate=>8,
                                                                                                        OffSet =>2
                                                                                                    }
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    LastValue=> 2048,
                                                                                    History     =>  {
                                                                                                        StartDate=>8,
                                                                                                        OffSet =>2
                                                                                                    }
                                                                                },
                                                                                {
                                                                                    RuleId =>40,
                                                                                    Percentile_95=> 2048,
                                                                                    Percentile_50 => 2048,
                                                                                    History     =>  {
                                                                                                        StartDate=>8,
                                                                                                        OffSetHours=> -2, 
                                                                                                        OffSet =>2
                                                                                                    }
                                                                                },
                                                                                {
                                                                                    RuleId =>42,
                                                                                    Percentile_95=>2048,
                                                                                    Percentile_50 => 2048,
                                                                                    History     =>  {
                                                                                                        StartDate=>8,
                                                                                                        OffSetHours=> -2, 
                                                                                                        OffSet =>2
                                                                                                    }
                                                                                },
                                                                                {
                                                                                    RuleId =>1,
                                                                                    LastValue=> 4096,
                                                                                    History     =>  {
                                                                                                        StartDate=>8,
                                                                                                        OffSetHours=> -2, 
                                                                                                        OffSet =>2
                                                                                                    }
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
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,$loop{Loop_VM1},"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED VMs WITH MULTIPLE DAILY DATA");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        while( $row = $sth->fetchrow_arrayref) {
            is (@{$row}[4],"1536.0","TESTING p_95ProcessorLoadOnHost VM Data CALCULATION");
            is (@{$row}[5],"1536.0","TESTING p_95UsedPhysicalRamInMB VM Data CALCULATION");
            is (@{$row}[6],"4096.0","TESTING Max_UsedVhdDiskSpaceInMB VM Data CALCULATION");
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
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,
                                                                IsRunning =>1,
                                                                ValidFromDate => SqlDate(-9),
                                                                DataLastSeen => SqlDate(0),
                                                                DailyData=> [
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
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED VM WITH UNPOPULATED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[2],"1.0","TESTING OnHostPecentage VM DATA CALCULATION");
        is (@{$row}[3],"1.0","TESTING RunningOnHostPecentage VM DATA CALCULATION");
        is (@{$row}[4],undef,"TESTING p_95ProcessorLoadOnHost VM UNPOPULATED DATA CALCULATION");
        is (@{$row}[5],undef,"TESTING p_95UsedPhysicalRamInMB VM UNPOPULATED DATA CALCULATION");
        is (@{$row}[6],undef,"TESTING Max_UsedVhdDiskSpaceInMB VM UNPOPULATED DATA CALCULATION");
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
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,
                                                                IsRunning =>1,
                                                                ValidFromDate => SqlDate(-9),
                                                                DataLastSeen => SqlDate(0),
                                                                DailyData=> [
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
    for( my $number = 1; $number != $loop{Loop_Cluster1}*$loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED VM WITH MISSING DAILY DATA");
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number),9);
        $row = $sth->fetchrow_arrayref;
        is (@{$row}[6],undef,"TESTING MISSING Max_UsedVhdDiskSpaceInMB VM Data");
    }
}

sub HostIdFromEntity
{
    my $managedId = shift @_;
    my $sth = $g_dbh1->prepare ("Select HostId from SavisionCloudReporter.HostManagedEntityMap where ManagedEntityRowId = $managedId");
    $sth->execute();
    return $sth->fetchrow_array;
}





