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


use CloudReporter::Environment qw(EnvironmentBuild SqlDate);
                    
use constant TEST_QUERY         =>  'exec Savision_CloudReporter_Report_HyperV_VirtualMachineNewlyCreated
                                     @objectList = ?,
                                     @CreatedWithinNDays = ?';
use constant DSN =>'dbi:ODBC:CloudReporterTest';
my $connection = DBIx::Connection->new(name => 'test', dsn => DSN, username => 'HEROES\mike', password => 'P@ssw0rd');
my $g_dbh1 = DBI->connect(DSN);

EmptyTableTest();
NonRecentVM(1,1,1);
All_VM_NotNew(2,1,1);
for( my $k = 1; $k < 5; $k++){
    for (my $j = 1; $j < 5; $j++){
        for (my $i = 0; $i < 5 ; $i++){
            unless ($j == 3 || $i == 3 ||$k ==3){
                All_VM_New($k,$j,$i);
            }
        }
    }
}
for (my $j = 1;$j<5;$j++){
    for (my $i = 1; $i<5; $i++){
        unless ($j == 3|| $i == 3){
            UnclusteredVM_New($j,$i);
        }
    }
}
VMWithMultiplePropertyLines(1,1,2);
ClusteredToUnclustered(1,2,3);
UnclusteredToClustered(1,2,3);
ChangedHosts(1,2,3);
FaultyData_OldToNew(1,2,3);
FaultyData_NewToOld(1,2,3);
BeforeThreshHold(1,1,1);
AfterThreshHold(1,1,1);
MixedThreshHold(1,1,1); 
done_testing();


sub EmptyTableTest
{
    my %Env = (
     );
    EnvironmentBuild (\%Env);
    my $sth = my $sth = RunTestQueryReturnRows(1,0);
    is($sth,0,"TESTING WITH EMPTY TABLES");
}

sub NonRecentVM
{
    my %Env = (
        Clusters =>  [
                        "Loop_Cluster1",
                        { 
                            InstalledMemoryMB => 0,
                            Hosts =>    [
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [   
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(-2),
                                                                DataLastSeen=>SqlDate(-99)
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},3);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,0,"TESTING WITH $Total NONRECENT VM");
}

sub All_VM_NotNew
{
    my %Env = (
        Clusters =>  [
                        "Loop_Cluster1",
                        { 
                            InstalledMemoryMB => 0,
                            Hosts =>    [
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [   
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(-9),
                                                                DataLastSeen=>SqlDate(0)
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1}, 3);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,0,"TESTING WITH $loop{Loop_VM1} OLD DISABLED VM's IN EACH $loop{Loop_Host1} HOSTS IN EACH $loop{Loop_Cluster1} CLUSTERS");
}

sub All_VM_New
{
    my %Env = (
        Clusters =>  [
                        "Loop_Cluster1",
                        { 
                            InstalledMemoryMB => 0,
                            Hosts =>    [
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [   
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(-2),
                                                                DataLastSeen=>SqlDate(0)
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},3);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $loop{Loop_VM1} NEW VM's IN EACH $loop{Loop_Host1} HOSTS IN EACH $loop{Loop_Cluster1} CLUSTERS");
}

sub UnclusteredVM_New
{
    my %Env = (
        Hosts =>    [
                        "Loop_Host1",
                        {     
                            VMs =>  [   
                                        "Loop_VM1",
                                        {
                                            ValidToDate => undef,                                                                                                                               
                                            DateTimeWhenInstalled => SqlDate(-2),
                                            DataLastSeen=>SqlDate(0)
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Host1},3);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $Total NEW UNCLUSTERED VM");
}

###Tests VMs with out of date property lines
###
###
sub VMWithMultiplePropertyLines
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
                                                                DateTimeWhenInstalled => SqlDate(0),
                                                                DataLastSeen=>SqlDate(0),
                                                                PastData => [
                                                                                {
                                                                                    ValidToDate => SqlDate(-1),
                                                                                    ValidFromDate => SqlDate(-7)
                                                                                }
                                                                            ]
                                                            },
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(-9),
                                                                DataLastSeen=>SqlDate(0),
                                                                PastData => [
                                                                                {
                                                                                    ValidToDate => SqlDate(-1),
                                                                                    ValidFromDate => SqlDate(-7)
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
                Loop_Cluster1   =>shift @_,
                Loop_Host1      =>shift @_,
                Loop_VM1        =>shift @_
                );
    EnvironmentBuild (\%Env,\%loop);
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $Total NEW FROM ".$Total*2 ." CLUSTERED VMs WITH MULTIPLE PROPERTY FIELDS");
}

### Tests Unclustered VMs previously clustered
###
###
sub ClusteredToUnclustered
{

    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        {
                        },
                        "End_Loop"
                    ],
        Hosts =>    [
                        "Loop_Host1",
                        {
                            PastData => {
                                            ClusterID =>(1),
                                            ValidToDate => SqlDate(-1),
                                            ValidFromDate => SqlDate(-7)
                                        },
                             
                            VMs =>  [   
                                        "Loop_VM1",
                                        {
                                            ValidToDate => undef,                                                                                                                               
                                            DateTimeWhenInstalled => SqlDate(0),
                                            DataLastSeen=>SqlDate(0),
                                            PastData => [
                                                            {
                                                            ClusterID =>(1),
                                                            ValidToDate => SqlDate(-1),
                                                            ValidFromDate => SqlDate(-7)
                                                            }
                                                        ]
                                        },
                                        "End_Loop"
                                    ],
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
    
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Host1},3);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $Total VMs MOVED FROM CLUSTERED TO UNCLUSTERED");
}

### Tests Clustered VMs previously Unclustered
###
###
sub UnclusteredToClustered
{
    my %Env = (
        Clusters=>  [
                        "Loop_Cluster1",
                        {
                            Hosts =>    [
                                            "Loop_Host1",
                                            {
                                                PastData=>  [
                                                                {   
                                                                    ClusterId =>undef,
                                                                    ValidToDate => SqlDate(-1),
                                                                    ValidFromDate => SqlDate(-7)
                                                                }
                                                            ],
                                                VMs => [
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(0),
                                                                DataLastSeen=>SqlDate(0),
                                                                PastData => [
                                                                                {
                                                                                    ClusterId =>undef,
                                                                                    ValidToDate => SqlDate(-1),
                                                                                    ValidFromDate => SqlDate(-7)
                                                                                }
                                                                            ],
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
    EnvironmentBuild(\%Env,\%loop);
    
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},3);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,$Total,"TESTING WITH $Total VMs MOVED FROM CLUSTERED TO UNCLUSTERED");
}


### Tests VMs that recently changed Hosts
###
###
sub ChangedHosts
{
    my %loop = (
            Loop_Cluster1   =>shift @_,
            Loop_Host1      =>shift @_,
            Loop_VM1        =>shift @_
            );
    my %Env =   (
        Clusters=>  [
                        "Loop_Cluster1",
                        {
                            Hosts =>    [
                                            "Loop_Host1",
                                            {
                                                VMs => [
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(0),
                                                                DataLastSeen=>SqlDate(0),
                                                                PastData => [
                                                                                {
                                                                                    HostId =>1,
                                                                                    ValidToDate => SqlDate(-1),
                                                                                    ValidFromDate => SqlDate(-7)
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

    EnvironmentBuild(\%Env,\%loop);
    

    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},3);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,$Total,"TESTING WITH $Total VMs THAT CHANGED HOSTS");
}

### Tests Unmonitored VMs previously Monitored
###
###
sub FaultyData_OldToNew
{
    my %Env = (
        Clusters=>  [
                        "Loop_Cluster1",
                        {
                            Hosts =>    [
                                            "Loop_Host1",
                                            {
                                                VMs => [
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(0),
                                                                DataLastSeen=>SqlDate(0),
                                                                PastData => [
                                                                                {
                                                                                    ValidToDate => undef,                                                                                                                               
                                                                                    DateTimeWhenInstalled => SqlDate(0),
                                                                                    DataLastSeen=>SqlDate(-9),
                                                                                }
                                                                            ],
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
    EnvironmentBuild(\%Env,\%loop);
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,$Total,"TESTING WITH $Total NEW VMs WHERE PAST DATA INDICATES OLD");
}

### Tests Monitored VMs previously Unmonitored
###
###
sub FaultyData_NewToOld
{
    my %Env = (
        Clusters=>  [
                        "Loop_Cluster1",
                        {
                            Hosts =>    [
                                            "Loop_Host1",
                                            {
                                                VMs => [
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(0),
                                                                DataLastSeen=>SqlDate(-9),
                                                                PastData => [
                                                                                {
                                                                                    ValidToDate => undef,                                                                                                                               
                                                                                    DateTimeWhenInstalled => SqlDate(0),
                                                                                    DataLastSeen=>SqlDate(0),
                                                                                }
                                                                            ],
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
    EnvironmentBuild(\%Env,\%loop);
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,0,"TESTING WITH $Total OLD VMs WHERE PAST DATA INDICATES NEW");
}

###Tests Vms off before the specified threshold
###
###
sub BeforeThreshHold
{
    my %Env = (
        Clusters =>  [
                        "Loop_Cluster1",
                        { 
                            InstalledMemoryMB => 0,
                            Hosts =>    [
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [   
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(-10),                                                               
                                                                DataLastSeen=>SqlDate(0)
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},9);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,0,"TESTING WITH $Total VM's OFF 1 DAY BEFORE THRESHOLD");
}

###Tests Vms off after the specified threshold
###
###
sub AfterThreshHold
{
    my %Env = (
        Clusters =>  [
                        "Loop_Cluster1",
                        { 
                            InstalledMemoryMB => 0,
                            Hosts =>    [
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [   
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(-8),                                                               
                                                                DataLastSeen=>SqlDate(0)
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},9);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $Total VM's OFF 1 DAY AFTER THRESHOLD");
}

### Tests VMs off before and after the specified threshold
###
###
sub MixedThreshHold
{
    my %Env = (
        Clusters =>  [
                        "Loop_Cluster1",
                        { 
                            InstalledMemoryMB => 0,
                            Hosts =>    [
                                            "Loop_Host1",
                                            {     
                                                VMs =>  [   
                                                            "Loop_VM1",
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(-8),                                                               
                                                                DataLastSeen=>SqlDate(0)
                                                            },
                                                            {
                                                                ValidToDate => undef,                                                                                                                               
                                                                DateTimeWhenInstalled => SqlDate(-10),                                                               
                                                                DataLastSeen=>SqlDate(0)
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},9);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $Total VMS OFF BEFORE AND $Total VMS OFF AFTER THRESHOLD");
}

sub RunTestQueryReturnRows
{
    my $NumberofHosts = shift @_;
    my $ForAtLeastDays = shift @_;
    my $sth;
    my $objectlist = 1;
    
    for (my $count = 2; $count < $NumberofHosts+1;$count++){
       $objectlist =$objectlist."|".$count; 
    }
    $sth = $g_dbh1->prepare(TEST_QUERY);
    $sth->execute($objectlist,$ForAtLeastDays);
    my $row;
    my $rows=0;
    while($row = $sth->fetchrow_arrayref) {
        $rows++;
    }
    return $rows;
}