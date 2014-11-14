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
                    
use constant TEST_QUERY         =>  'exec Savision_CloudReporter_Report_HyperV_VirtualMachinePoweredOff
                                     @objectList = ?
                                    ,@ForAtLeastNDays = ?';
use constant DSN =>'dbi:ODBC:CloudReporterTest';
my $connection = DBIx::Connection->new(name => 'test', dsn => DSN, username => 'HEROES\mike', password => 'P@ssw0rd');
my $g_dbh1 = DBI->connect(DSN);
my $g_dbh2 = DBI->connect(DSN);
my $database_counter = 1;




EmptyTableTest();
NonRecentVM(1,1,1);
NonRecentVM(1,1,2);
NonRecentVM(1,1,4);
for (my $j = 1;$j<5;$j++){
    for (my $i = 1; $i<5; $i++){
        unless ($j == 3|| $i == 3){
            HostsWithOffVirtualMachines($j,$i);
        }
    }
}

for( my $k = 1; $k < 5; $k++){
    for (my $j = 1; $j < 5; $j++){
        for (my $i = 0; $i < 5 ; $i++){
            unless ($j == 3 || $i == 3 ||$k ==3){
                AllVirtualMachinesOff($k,$j,$i);
            }
        }
    }
}
VMWithMultiplePropertyLines(1,1,2);
ClusteredToUnclustered(1,1,1);
UnclusteredToClustered(1,1,1);
ChangedHosts(2,2,1);
PoweredOnToPoweredOff(1,1,1);
PoweredOffToPoweredOn(1,1,1);
BeforeThreshHold(1,1,1);
AfterThreshHold(1,1,1);
MixedThreshHold(1,1,1);    
done_testing();



###Tests for an empty table 
###
###
sub EmptyTableTest
{
    my %Env = (
     );
    EnvironmentBuild (\%Env);
    my $sth = my $sth = RunTestQueryReturnRows(1,0);
    is($sth,0,"TESTING WITH EMPTY TABLES");
}

###Tests for a table with VM's without recent data (Recent = after yesterday)
###
###
sub NonRecentVM
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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-1),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,0,"TESTING WITH $Total NONRECENT VM");
}

###Tests for a table with only running VMs 
###
###
sub RunningVM
{
    my %Env = (
        Clusters => [
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
                                                                DateTimeWhenStoppedRunning => SqlDate(-1),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,0,"TESTING WITH $Total RUNNING VM");
}

###Tests using unclustered hosts and off VMs 
###
###
sub HostsWithOffVirtualMachines
{
    my %Env = (
        Hosts =>    [
                        "Loop_Host1",
                        {     
                            VMs =>  [   
                                        "Loop_VM1",
                                        {
                                            IsRunning => 0,
                                            ValidToDate => undef,
                                            DateTimeWhenStoppedRunning => SqlDate(-10),
                                            DailyData =>[
                                                            {
                                                                RuleId=>1 
                                                            }
                                                        ],
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Host1},5);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $Total OFF UNCLUSTERED VM");

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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
                                                                DataLastSeen=>SqlDate(0),
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
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
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
    is($TestResult,$Total,"TESTING WITH $Total OFF FROM ".$Total*2 ." CLUSTERED VMs WITH MULTIPLE PROPERTY FIELDS");
}

###Tests Clustered Hosts with Off Vms 
###
###
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
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
    my $Total = $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1};
    is($TestResult,$Total,"TESTING WITH $loop{Loop_VM1} OFF VM's IN EACH $loop{Loop_Host1} HOSTS IN EACH $loop{Loop_Cluster1} CLUSTERS");
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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
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
    is($TestResult,$Total,"TESTING WITH $Total VM's OFF 1 DAY BEFORE THRESHOLD");
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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-8),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
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
    is($TestResult,0,"TESTING WITH $Total VM's OFF 1 DAY AFTER THRESHOLD");
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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-8),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
                                                                DataLastSeen=>SqlDate(0)
                                                            },
                                                            {
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
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
                                            IsRunning => 0,
                                            ValidToDate => undef,
                                            DateTimeWhenStoppedRunning => SqlDate(-10),
                                            DailyData =>[
                                                            {
                                                                RuleId=>1 
                                                            }
                                                        ],
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
    
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
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
    
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
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
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
                                                                DataLastSeen=>SqlDate(0),
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,0,"TESTING WITH $Total VMs POWERED ON AFTER POWERED OFF");
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
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
                                                                DataLastSeen=>SqlDate(0),
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
    my $TestResult = RunTestQueryReturnRows($loop{Loop_Cluster1}*$loop{Loop_Host1},5);
    my $Total = $loop{Loop_Host1}*$loop{Loop_VM1}*$loop{Loop_Cluster1};
    is($TestResult,$Total,"TESTING WITH $Total VMs POWERED OFF AFTER POWERED ON");
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
                                                                IsRunning => 0,
                                                                ValidToDate => undef,
                                                                DateTimeWhenStoppedRunning => SqlDate(-10),
                                                                DailyData =>[
                                                                                {
                                                                                    RuleId=>1 
                                                                                }
                                                                            ],
                                                                DataLastSeen=>SqlDate(0),
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

### Testing function returning number of rows after report is run
###
###
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
    $database_counter++;
    return $rows;
}



