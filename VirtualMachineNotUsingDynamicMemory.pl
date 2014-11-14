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
                    
use constant TEST_QUERY         =>  'exec Savision_CloudReporter_Report_HyperV_VirtualMachineNotUsingDynamicMemory
                                     @objectList = ?';
use constant DSN =>'dbi:ODBC:CloudReporterTest';
my $connection = DBIx::Connection->new(name => 'test', dsn => DSN, username => 'HEROES\mike', password => 'P@ssw0rd');
my $g_dbh1 = DBI->connect(DSN);

EmptyTableTest();
NonRecentVM(2,2,2);

for (my $j = 1;$j<5;$j++){
    for (my $i = 1; $i<5; $i++){
        unless ($j == 3|| $i == 3){
            UnclusteredVM_DynamicMemeoryDisabled($j,$i);
        }
    }
}

for( my $k = 1; $k < 5; $k++){
    for (my $j = 1; $j < 5; $j++){
        for (my $i = 0; $i < 5 ; $i++){
            unless ($j == 3 || $i == 3 ||$k ==3){
                All_VM_DynamicMemeoryDisabled($k,$j,$i);
            }
        }
    }
}

for( my $k = 1; $k < 2; $k++){
    for (my $j = 1; $j < 2; $j++){
        for (my $i = 0; $i < 2 ; $i++){
            AllVirtualMachinesOff($k,$j,$i);
        }
    }
}
All_VM_DynamicMemeoryEnabled(2,1,1);
VMWithMultiplePropertyLines_DynamicMemoryDisabled(1,1,2);
ClusteredToUnclustered(1,2,3);
UnclusteredToClustered(1,2,3);
PoweredOffToPoweredOn(1,2,3);
PoweredOnToPoweredOff(1,2,3);
ChangedHosts(1,2,3);
DynamicMemoryDisabled_To_Enabled(1,2,3);
DynamicMemoryEnabled_To_Disabled(1,2,3);




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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled=> 0,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,0,"TESTING WITH $Total NONRECENT VM");
}

sub All_VM_DynamicMemeoryEnabled
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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>1,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
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
    EnvironmentBuild (\%Env,\%loop);
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,0,"TESTING WITH $loop{Loop_VM1} DYNAMIC MEMORY DISABLED VM's IN EACH $loop{Loop_Host1} HOSTS IN EACH $loop{Loop_Cluster1} CLUSTERS");
}

sub AllVirtualMachinesOff
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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                RuleId =>1,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
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
        
    EnvironmentBuild (\%Env,\%loop);
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,0,"TESTING WITH $loop{Loop_VM1} OFF VM's IN EACH $loop{Loop_Host1} HOSTS IN EACH $loop{Loop_Cluster1} CLUSTERS");
}

sub All_VM_DynamicMemeoryDisabled
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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>0,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
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
    EnvironmentBuild (\%Env,\%loop);
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $loop{Loop_VM1} DYNAMIC MEMORY DISABLED VM's IN EACH $loop{Loop_Host1} HOSTS IN EACH $loop{Loop_Cluster1} CLUSTERS");
}

sub UnclusteredVM_DynamicMemeoryDisabled
{
    my %Env = (
        Hosts =>    [
                        "Loop_Host1",
                        {     
                            VMs =>  [   
                                        "Loop_VM1",
                                        {
                                            IsRunning => 1,
                                            ValidToDate => undef,
                                            IsDynamicMemoryEnabled =>0,
                                            DateTimeWhenStoppedRunning => undef,
                                            DataLastSeen=>SqlDate(0),
                                            DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Host1});
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $Total DYNAMIC MEMORY DISABLED UNCLUSTERED VM");
}

###Tests VMs with out of date property lines
###
###
sub VMWithMultiplePropertyLines_DynamicMemoryDisabled
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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>1,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
                                                                PastData => [
                                                                                {
                                                                                    ValidToDate => SqlDate(-1),
                                                                                    ValidFromDate => SqlDate(-7)
                                                                                }
                                                                            ]
                                                            },
                                                            {
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>0,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
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
    is($TestResult,$Total,"TESTING WITH $Total DYNAMIC MEMORY DISABLED FROM ".$Total*2 ." CLUSTERED VMs WITH MULTIPLE PROPERTY FIELDS");
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
                                            IsRunning => 1,
                                            ValidToDate => undef,
                                            IsDynamicMemoryEnabled =>0,
                                            DateTimeWhenStoppedRunning => undef,
                                            DataLastSeen=>SqlDate(0),
                                            DailyData =>[
                                                            {
                                                                RuleId=>42 
                                                            }
                                                        ],
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
    
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Host1});
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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>0,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
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
    
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,$Total,"TESTING WITH $Total VMs MOVED FROM CLUSTERED TO UNCLUSTERED");
}

### Tests on VMs previously off
###
###
sub PoweredOffToPoweredOn
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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>0,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
                                                                PastData => [
                                                                                {
                                                                                    IsRunning => 0,
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,$Total,"TESTING WITH $Total VMs POWERED ON AFTER POWERED OFF");
}

### Tests Off VMs previously On
###
###
sub PoweredOnToPoweredOff
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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>0,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
                                                                PastData => [
                                                                                {
                                                                                    IsRunning => 1,
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
    EnvironmentBuild(\%Env,\%loop);
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,0,"TESTING WITH $Total VMs POWERED OFF AFTER POWERED ON");
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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>0,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
                                                                PastData => [
                                                                                {
                                                                                    HostId =>1,
                                                                                    IsRunning => 1,
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
    

    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,$Total,"TESTING WITH $Total VMs THAT CHANGED HOSTS");
}

### Tests Unmonitored VMs previously Monitored
###
###
sub DynamicMemoryDisabled_To_Enabled
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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>0,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
                                                                PastData => [
                                                                                {
                                                                                    IsDynamicMemoryEnabled =>1,
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,$Total,"TESTING WITH $Total VMs DYNAMIC MEMORY ENABLED AFTER DYNAMIC MEMORY DISABLED");
}

### Tests Monitored VMs previously Unmonitored
###
###
sub DynamicMemoryEnabled_To_Disabled
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
                                                                IsRunning => 1,
                                                                ValidToDate => undef,
                                                                IsDynamicMemoryEnabled =>1,
                                                                DateTimeWhenStoppedRunning => undef,
                                                                DataLastSeen=>SqlDate(0),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>42 
                                                                                }
                                                                            ],
                                                                PastData => [
                                                                                {                                                                                    
                                                                                    IsDynamicMemoryEnabled =>0,
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
    EnvironmentBuild(\%Env,\%loop);
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1});
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,0,"TESTING WITH $Total VMs DYNAMIC MEMORY DISABLED AFTER DYNAMIC MEMORY ENABLED");
}




sub RunTestQueryReturnRows
{
    my $NumberofHosts = shift @_;
    my $sth;
    my $objectlist = 1;
    
    for (my $count = 2; $count < $NumberofHosts+1;$count++){
       $objectlist =$objectlist."|".$count; 
    }
    $sth = $g_dbh1->prepare(TEST_QUERY);
    $sth->execute($objectlist);
    my $row;
    my $rows=0;
    while($row = $sth->fetchrow_arrayref) {
        $rows++;
    }
    return $rows;
}