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
use CloudReporter::Build qw(Reset
                    ClusterPopulate
                    ClusterPropertyPopulate
                    ClusterId 
                    ClusterManagedEntityMapPopulate
                    HostPopulate
                    HostId 
                    HostPropertyPopulate 
                    HostManagedEntityMapPopulate 
                    VMPopulate VMId VMPropertyPopulate 
                    VMManagedEntityMapPopulate 
                    VMMAggregateDailyPopulate 
                    ClusterAggregateDailyPopulate
                    ClusterStoragePopulate
                    ClusterStorageManagedEntityMapPopulate
                    ClusterStoragePropertyPopulate);
use CloudReporter::Environment qw(EnvironmentBuild SqlDate);
                    
use constant TEST_QUERY         =>  'exec Savision_CloudReporter_Report_HyperV_VirtualMachinePoweredOff
                                     @objectList = ?
                                    ,@ForAtLeastNDays = ?';
use constant DSN =>'dbi:ODBC:CloudReporterTest';

my $connection = DBIx::Connection->new(name => 'test', dsn => DSN, username => 'HEROES\mike', password => 'P@ssw0rd');
my $g_dbh1 = DBI->connect(DSN);
my $g_dbh2 = DBI->connect(DSN);
my $database_counter = 1;


#Regular Insert Tests

JustClusters();
JustClusterLoops(1);
HostsinClusters();
HostsinClusterLoops(1,1);
ClusterStorageTest();
ClusterStorageTestLoops(1,1);
ClusteredVMTest();
ClusteredVMTestLoops(1,1,1);
JustHost();
JustHostLoops(1);
UnclusteredVMTest();
UnclusteredVMTestLoops(1,1);

#Historical Daily Data Tests
ClusterHistoricalData();
HostHistoricalData();
VMHistoricalData();
ClusterStorageHistoricalData();


#Multiple property Lines Tests

MultiplePropertyClusters();
MultiplePropertyClustersLoops(1);
MultiplePropertyClusteredHosts();
MultiplePropertyClusteredHostLoops(1,1);
MultiplePropertyClusterStorage();
MultiplePropertyClusterStorageLoops(1,1);
MultiplePropertyClusteredVM(); 
MultiplePropertyClusteredVMLoops(1,1,1);
MultiplePropertyUnclusteredHosts();
MultiplePropertyUnclusteredHostLoops(1);
MultiplePropertyUnclusteredVM();
MultiplePropertyUnclusteredVMLoops(1,1);


#nested Looping tests
LoopNestTest(2,2,2,2);
ComplexEnvironmentTest(2,2,2,2,2,2,2,2);

done_testing();

sub ClusterHistoricalData
{
    my %Env = (
        Clusters =>     [   
                            {   
                                DailyData           =>  [
                                                            {
                                                                RuleId =>3,
                                                                History =>  {
                                                                                StartDate =>9,
                                                                                OffSetHours => -2,
                                                                                OffSet=>3
                                                                            }
                                                            },
                                                            {
                                                                RuleId =>4,
                                                                History =>  {
                                                                                StartDate =>8,
                                                                                OffSet=>3
                                                                            }
                                                            }
                                                        ]
                            }
                            
                        ]
    );
    EnvironmentBuild (\%Env);
    
    my $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vClusterAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-3,-2));
    is (@{$sth->fetchrow_arrayref}[0],3, "TESTING HISTORICAL CLUSTER DAILY DATA INSERT");
    
    $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vClusterAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-2));
    is (@{$sth->fetchrow_arrayref}[0],4, "TESTING ALTERNATING HISTORICAL CLUSTER DAILY DATA INSERT");
}

sub HostHistoricalData
{
    my %Env = (
        Hosts =>     [   
                            {   
                                DailyData           =>  [
                                                            {
                                                                RuleId =>3,
                                                                History =>  {
                                                                                StartDate =>9,
                                                                                OffSetHours => -2,
                                                                                OffSet=>3
                                                                            }
                                                            },
                                                            {
                                                                RuleId =>4,
                                                                History =>  {
                                                                                StartDate =>8,
                                                                                OffSet=>3
                                                                            }
                                                            }
                                                        ]
                            }
                            
                        ]
    );
    EnvironmentBuild (\%Env);
    
    my $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vHostAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-3,-2));
    is (@{$sth->fetchrow_arrayref}[0],3, "TESTING HISTORICAL UNCLUSTERED HOST DAILY DATA INSERT");
    
    $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vHostAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-2));
    is (@{$sth->fetchrow_arrayref}[0],4, "TESTING ALTERNATING HISTORICAL UNCLUSTERED HOST DAILY DATA INSERT");
    
    %Env = (
        Clusters=>[
                    {
                        Hosts =>    [   
                                        {   
                                            DailyData           =>  [
                                                                        {
                                                                            RuleId =>3,
                                                                            History =>  {
                                                                                            StartDate =>9,
                                                                                            OffSetHours => -2,
                                                                                            OffSet=>2
                                                                                        }
                                                                        },
                                                                        {
                                                                            RuleId =>4,
                                                                            History =>  {
                                                                                            StartDate =>8,
                                                                                            OffSet=>2
                                                                                        }
                                                                        }
                                                                    ]
                                        }
                                        
                                    ]
                    }
                ]
    );
    EnvironmentBuild (\%Env);
    
    $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vHostAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-3,-2));
    is (@{$sth->fetchrow_arrayref}[0],3, "TESTING HISTORICAL CLUSTERED HOST DAILY DATA INSERT");
    
    $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vHostAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-2));
    is (@{$sth->fetchrow_arrayref}[0],4, "TESTING ALTERNATING HISTORICAL CLUSTERED HOST DAILY DATA INSERT");


}

sub VMHistoricalData
{
    my %Env = (
        Hosts =>     [   
                            {   
                                VMs=>    [
                                            {
                                                DailyData   =>  [
                                                                    {
                                                                        RuleId =>3,
                                                                        History =>  {
                                                                                        StartDate =>9,
                                                                                        OffSetHours => -2,
                                                                                        OffSet=>3
                                                                                    }
                                                                    },
                                                                    {
                                                                        RuleId =>4,
                                                                        History =>  {
                                                                                        StartDate =>8,
                                                                                        OffSet=>3
                                                                                    }
                                                                    }
                                                                ]
                                            }
                                        ]
                            }
                            
                        ]
    );
    EnvironmentBuild (\%Env);
    
    my $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vVMAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-3,-2));
    is (@{$sth->fetchrow_arrayref}[0],3, "TESTING HISTORICAL UNCLUSTERED VM DAILY DATA INSERT");
    
    $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vVMAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-2));
    is (@{$sth->fetchrow_arrayref}[0],4, "TESTING ALTERNATING HISTORICAL UNCLUSTERED VM DAILY DATA INSERT");
    
    %Env = (
        Clusters=>[
                    {
                        Hosts =>    [   
                                        {   
                                            VMs=>    [
                                                        {
                                                            DailyData   =>  [
                                                                                {
                                                                                    RuleId =>3,
                                                                                    History =>  {
                                                                                                    StartDate =>9,
                                                                                                    OffSetHours => -2,
                                                                                                    OffSet=>3
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>4,
                                                                                    History =>  {
                                                                                                    StartDate =>8,
                                                                                                    OffSet=>3
                                                                                                }
                                                                                }
                                                                            ]
                                                        }
                                                    ]
                                        }
                                        
                                    ]
                    }
                ]
    );
    EnvironmentBuild (\%Env);
    
    $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vVMAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-3,-2));
    is (@{$sth->fetchrow_arrayref}[0],3, "TESTING HISTORICAL CLUSTERED VM DAILY DATA INSERT");
    
    $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vVMAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-2));
    is (@{$sth->fetchrow_arrayref}[0],4, "TESTING ALTERNATING HISTORICAL CLUSTERED VM DAILY DATA INSERT");


}

sub ClusterStorageHistoricalData
{
    my  %Env = (
        Clusters=>[
                    {
                        ClusterStorage =>   [   
                                                {   
                                                    DailyData           =>  [
                                                                                {
                                                                                    RuleId =>3,
                                                                                    History =>  {
                                                                                                    StartDate =>9,
                                                                                                    OffSetHours => -2,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                },
                                                                                {
                                                                                    RuleId =>4,
                                                                                    History =>  {
                                                                                                    StartDate =>8,
                                                                                                    OffSet=>2
                                                                                                }
                                                                                }
                                                                            ]
                                                }
                                                
                                            ]
                    }
                ]
    );
    EnvironmentBuild (\%Env);
    
    my $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vClusterStorageAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-3,-2));
    is (@{$sth->fetchrow_arrayref}[0],3, "TESTING HISTORICAL CLUSTERED CLUSTERSTORAGE DAILY DATA INSERT");
    
    $sth = $g_dbh1->prepare("Select RuleId from [SavisionCloudReporter].[vClusterStorageAggregateDaily] where DateTime = ?");
    $sth->execute(SqlDate(-2));
    is (@{$sth->fetchrow_arrayref}[0],4, "TESTING ALTERNATING HISTORICAL CLUSTERED CLUSTERSTORAGE DAILY DATA INSERT");
    EnvironmentBuild (\%Env);

}

sub ComplexEnvironmentTest
{
    my %Env = (
        Clusters=>  [
                        "Loop_Cluster1",
                        {   
                            InstalledMemoryMB => 0,
                            ValidToDate => undef,
                            ValidFromDate => SqlDate(-4),
                            PastProperties => [
                                            {
                                                InstalledMemoryMB => 4500,
                                                ValidToDate => SqlDate(-4),
                                                ValidFromDate => SqlDate(-10),
                                            }
                                        ],
                            Hosts   =>  [
                                            "Loop_Host1",
                                            {
                                                DataLastSeen => SqlDate(0),
                                                InstalledMemoryMB=> 2345,
                                                ValidToDate => undef,
                                                ValidFromDate=> SqlDate(-4),
                                                PastProperties=>  [   
                                                                {
                                                                    InstalledMemoryMB=> 2345,
                                                                    ValidToDate => SqlDate(-4),
                                                                    ValidFromDate=> SqlDate(-8),
                                                                },
                                                            ],
                                                VMs=>       [
                                                                "Loop_VM1",
                                                                {
                                                                    IsRunning =>1 ,
                                                                    ValidToDate => undef,
                                                                    ValidFromDate=> SqlDate(-8),
                                                                    PastProperties=>  [
                                                                                    {
                                                                                        IsRunning =>0 ,
                                                                                        ValidToDate => SqlDate(-8),
                                                                                        ValidFromDate=> SqlDate(-14),
                                                                                    }
                                                                                ]
                                                                    
                                                                },
                                                                "End_Loop"  
                                                            ]
                                                                
                                            },
                                            "End_Loop"
                                        ]
                                        
                                        
                            
                        },
                        {
                            ValidToDate=>undef,
                            ClusterStorage=>    [     
                                                "Loop_Storage1",
                                                {
                                                    InstalledDiskSpaceMB => 10008,
                                                    PastProperties=>  [
                                                                    {
                                                                        InstalledDiskSpaceMB =>12345
                                                                    }
                                                                ]
                                                },
                                                "End_Loop"
                                            ]
                        },
                        "End_Loop",
                        "Loop_Cluster2",
                        {
                            InstalledMemoryMB => 0,
                            ValidToDate => undef,
                            ValidFromDate => SqlDate(-4),
                            PastProperties => [
                                            {
                                                InstalledMemoryMB => 4500,
                                                ValidToDate => SqlDate(-4),
                                                ValidFromDate => SqlDate(-10),
                                            }
                                        ],
                            Hosts   =>  [
                                            "Loop_Host2",
                                            {
                                                DataLastSeen => SqlDate(0),
                                                InstalledMemoryMB=> 2345,
                                                ValidToDate => undef,
                                                ValidFromDate=> SqlDate(-4),
                                                PastProperties=>  [   
                                                                {
                                                                    InstalledMemoryMB=> 2345,
                                                                    ValidToDate => SqlDate(-4),
                                                                    ValidFromDate=> SqlDate(-8),
                                                                },
                                                            ],
                                                VMs=>       [
                                                                "Loop_VM2",
                                                                {
                                                                    IsRunning =>1 ,
                                                                    ValidToDate => undef,
                                                                    ValidFromDate=> SqlDate(-8),
                                                                    PastProperties=>  [
                                                                                    {
                                                                                        IsRunning =>0 ,
                                                                                        ValidToDate => SqlDate(-8),
                                                                                        ValidFromDate=> SqlDate(-14),
                                                                                    }
                                                                                ]
                                                                    
                                                                },
                                                                "End_Loop"  
                                                            ]
                                                                
                                            },
                                            {
                                                DataLastSeen => SqlDate(0),
                                                InstalledMemoryMB=> 2345,
                                                ValidToDate => undef,
                                                ValidFromDate=> SqlDate(-4),
                                                PastProperties=>  [   
                                                                {
                                                                    InstalledMemoryMB=> 2345,
                                                                    ValidToDate => SqlDate(-4),
                                                                    ValidFromDate=> SqlDate(-8),
                                                                },
                                                            ],
                                                VMs=>       [
                                                                "Loop_VM3",
                                                                {
                                                                    IsRunning =>1 ,
                                                                    ValidToDate => undef,
                                                                    ValidFromDate=> SqlDate(-8),
                                                                    PastProperties=>    [
                                                                                            {
                                                                                                IsRunning =>0 ,
                                                                                                ValidToDate => SqlDate(-8),
                                                                                                ValidFromDate=> SqlDate(-14),
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
            Loop_Cluster1=>shift @_,
            Loop_Cluster2=>shift @_,
            Loop_Host1=>shift @_,
            Loop_Host2=>shift @_,
            Loop_VM1=>shift @_,
            Loop_VM2=>shift @_,
            Loop_VM3=>shift @_,
            Loop_Storage1=>shift @_
    );
    EnvironmentBuild(\%Env,\%loop);

    my $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Cluster");
    $sth ->execute();
    is ( $sth->fetchrow_array() , (2*$loop{Loop_Cluster1})+$loop{Loop_Cluster2}, "TESTING CLUSTER INSERT IN COMPLEX ENVIRONMENT");
    
    $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Host");
    $sth ->execute();
    is ( $sth->fetchrow_array(), (2*$loop{Loop_Cluster2}*$loop{Loop_Host2})+$loop{Loop_Cluster1}*$loop{Loop_Host1}, "TESTING HOST INSERT IN COMPLEX ENVIRONMENT");
    
    $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.VM");
    $sth ->execute();
    is ( $sth->fetchrow_array(),($loop{Loop_Cluster2}*$loop{Loop_Host2}*$loop{Loop_VM2})+($loop{Loop_Cluster2}*$loop{Loop_Host2}*$loop{Loop_VM3})+($loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1}),"TESTING VM INSERT IN COMPLEX ENVIRONMENT");
    
    $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.ClusterStorage");
    $sth ->execute();
    is ( $sth->fetchrow_array(),$loop{Loop_Cluster1}*$loop{Loop_Storage1},"TESTING CLUSTER STORAGE INSERT IN COMPLEX ENVIRONMENT");




}

sub LoopNestTest
{
    my %Env = (
        Clusters =>     [   
                            "Loop_Cluster1",
                            {   
                                Hosts =>    [   
                                                "Loop_Host1",
                                                {     
                                                    VMs =>  [
                                                                "Loop_VM1",
                                                                {

                                                                },
                                                                "End_Loop"
                                                            ]
                                                },
                                                "End_Loop"
                                            ],
                                ClusterStorage  =>  [
                                                        "Loop_Storage1",
                                                        {
                                                        },
                                                        "End_Loop"
                                                    ]
                            },
                            "End_Loop"
                        ]
    );
    
    my %loop =  (
                    Loop_Cluster1   => @_[0],
                    Loop_Host1      => @_[1],    
                    Loop_VM1        => @_[2],
                    Loop_Storage1   => @_[3],
                );
    EnvironmentBuild(\%Env,\%loop);
    
    my $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Cluster");
    $sth ->execute();
    is ( $sth->fetchrow_array() , $loop{Loop_Cluster1}, "TESTING CLUSTER LOOPS");
    
    $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Host");
    $sth ->execute();
    is ( $sth->fetchrow_array(), $loop{Loop_Cluster1}*$loop{Loop_Host1}, "TESTING HOST LOOP NESTED IN CLUSTER LOOP");
    
    
    $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.ClusterStorage");
    $sth ->execute();
    is ( $sth->fetchrow_array(), $loop{Loop_Cluster1}*$loop{Loop_Storage1}, "TESTING CLUSTER STORAGE LOOP NESTED IN CLUSTER LOOP");
    
    $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.VM");
    $sth ->execute();
    is ( $sth->fetchrow_array(), $loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1}, "TESTING VM LOOP NESTED IN HOST LOOP IN CLUSTER LOOP");
    

}

sub ClusterStorageTestLoops
{
    my %Env = (
        Clusters =>     [   
                            "Loop_Cluster1",
                            {   
                                InstalledMemoryMB => 0,
                                ClusterStorage =>   [   
                                                        "Loop_Storage1",
                                                        {
                                                            InstalledDiskSpaceMB => 10008,
                                                            DailyData           =>  [
                                                                                        {
                                                                                            RuleId=>3
                                                                                        }
                                                                                    ]
                                                        },
                                                        "End_Loop"
                                                    ],
                            },
                            "End_Loop"
                        ]
    );

    
    my %loop =  (
                    Loop_Cluster1   => @_[0],
                    Loop_Storage1   => @_[1],
                );
    EnvironmentBuild (\%Env,\%loop);

    my $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.ClusterStorage");
    $sth ->execute();
    is ( $sth->fetchrow_array(), $loop{Loop_Cluster1}*$loop{Loop_Storage1}, "TESTING CLUSTER STORAGE INSERT WITH LOOPS");
    
    $sth = $g_dbh1->prepare(" select InstalledDiskSpaceMB from  (select InstalledDiskSpaceMB , ROW_NUMBER() OVER (Order by ClusterStoragePropertiesId) As RowNumber from SavisionCloudReporter.ClusterStorageProperties) T where T.RowNumber = $loop{Loop_Cluster1}*$loop{Loop_Storage1}");
    $sth ->execute();
    is ( $sth->fetchrow_array(), 10008, "TESTING CONFIGURE CLUSTER STORAGE WITH LOOPS");
    
    $sth = $g_dbh1->prepare(" select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by ClusterStorageId) As RowNumber from SavisionCloudReporter.vClusterStorageAggregateDaily) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 3, "TESTING CONFIGURED CLUSTER STORAGE AGGREGATE DAILY PROPERTIES TABLE POPULATE WITHOUT LOOPS");
}

sub ClusterStorageTest
{
    my %Env = (
        Clusters =>     [   
                            {   
                                InstalledMemoryMB => 0,
                                ClusterStorage =>   [   
                                                        {
                                                            InstalledDiskSpaceMB => 10008,
                                                            DailyData           =>  [
                                                                                        {
                                                                                            RuleId=>3
                                                                                        }
                                                                                    ]
                                                        },
                                                    ],
                            }
                        ]
    );

    EnvironmentBuild (\%Env);

    my $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.ClusterStorage");
    $sth ->execute();
    is ( $sth->fetchrow_array(),1, "TESTING CLUSTER STORAGE INSERT WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare(" select InstalledDiskSpaceMB from  (select InstalledDiskSpaceMB , ROW_NUMBER() OVER (Order by ClusterStoragePropertiesId) As RowNumber from SavisionCloudReporter.ClusterStorageProperties) T where T.RowNumber = 1");
    $sth ->execute();
    is ( $sth->fetchrow_array(), 10008, "TESTING CONFIGURE CLUSTER STORAGE WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare(" select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by ClusterStorageId) As RowNumber from SavisionCloudReporter.vClusterStorageAggregateDaily) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 3, "TESTING CONFIGURED CLUSTER STORAGE AGGREGATE DAILY PROPERTIES TABLE POPULATE WITHOUT LOOPS");
}

sub JustClusters
{
    my $date = SqlDate(-9);
    my %Env = (
        Clusters =>     [   
                            {   
                                DataFirstSeen       =>$date,    
                                InstalledMemoryMB   => 5678,
                                DailyData           =>  [
                                                            {
                                                                RuleId =>3
                                                            }
                                                        ]
                            }
                            
                        ]
    );

    EnvironmentBuild (\%Env);

    my $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Cluster");
    $sth ->execute();
    is ( $sth->fetchrow_array()  , 1, "TESTING CLUSTER INSERT WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare(" select DataFirstSeen from  (select DataFirstSeen , ROW_NUMBER() OVER (Order by ClusterId) As RowNumber from SavisionCloudReporter.Cluster) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), $date,"TESTING CONFIGURE CLUSTER TABLE POPULATE WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare(" select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by ClusterId) As RowNumber from SavisionCloudReporter.ClusterProperties) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(),5678, "TESTING CONFIGURED CLUSTER PROPERTIES TABLE POPULATE WITHOUT LOOPS");

    $sth = $g_dbh1->prepare(" select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by ClusterId) As RowNumber from SavisionCloudReporter.vClusterAggregateDaily) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 3, "TESTING CONFIGURED CLUSTER AGGREGATE DAILY PROPERTIES TABLE POPULATE WITHOUT LOOPS");
}

sub JustClusterLoops
{
    my $date = SqlDate(-9);
    my %Env = (
        Clusters =>     [   
                            "Loop_Cluster1",
                            {   
                                DataFirstSeen       =>$date,    
                                DailyData           =>  [
                                                            {
                                                                RuleId =>3
                                                            }
                                                        ],
                                InstalledMemoryMB   => 5678,
                            },
                            "End_Loop"
                        ]
    );
    my %loop = (
                   Loop_Cluster1 => shift @_
                );
    EnvironmentBuild (\%Env,\%loop);
    
    my $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Cluster");
    $sth ->execute();
    is ( $sth->fetchrow_array() , $loop{Loop_Cluster1}, "TESTING CLUSTER INSERT WITH LOOPS");
    
    $sth = $g_dbh1->prepare(" select DataFirstSeen from  (select DataFirstSeen , ROW_NUMBER() OVER (Order by ClusterId) As RowNumber from SavisionCloudReporter.Cluster) T where T.RowNumber = $loop{Loop_Cluster1}");
    $sth ->execute();
    is ($sth->fetchrow_array(), $date,"TESTING CONFIGURE CLUSTER TABLE POPULATE WITH LOOPS");
    
    $sth = $g_dbh1->prepare(" select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by ClusterPropertiesId) As RowNumber from SavisionCloudReporter.ClusterProperties) T where T.RowNumber = $loop{Loop_Cluster1}");
    $sth ->execute();
    is ( $sth->fetchrow_array(),5678, "TESTING CONFIGURED CLUSTER PROPERTIES TABLE POPULATE WITH LOOPS");
    
    $sth = $g_dbh1->prepare(" select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by ClusterId) As RowNumber from SavisionCloudReporter.vClusterAggregateDaily) T where T.RowNumber = $loop{Loop_Cluster1}");
    $sth ->execute();
    is ($sth->fetchrow_array(), 3, "TESTING CONFIGURED CLUSTER AGGREGATE DAILY PROPERTIES TABLE POPULATE WITH LOOPS");

}

sub HostsinClusterLoops
{
    my $date = SqlDate(0);
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [
                                            "Loop_Host1",
                                            {     
                                                DataLastSeen => $date,
                                                InstalledMemoryMB => 12754,
                                                DailyData           =>  [
                                                        {
                                                            RuleId =>3
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
                    Loop_Cluster1   => @_[0],
                    Loop_Host1      => @_[1]
                );
                
    EnvironmentBuild (\%Env,\%loop);
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Host");
    $sth ->execute();
    is ($sth->fetchrow_array(),1,"TESTING CLUSTERED HOST INSERT WITH LOOPS");

    $sth = $g_dbh1->prepare(" select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by HostId) As RowNumber from SavisionCloudReporter.HostProperties) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 12754, "TESTING CONFIGURING CLUSTERED HOSTS INSERT WITH LOOPS ");
    
    $sth = $g_dbh1->prepare(" select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by HostId) As RowNumber from SavisionCloudReporter.vHostAggregateDaily) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 3, "TESTING CONFIGURED CLUSTERED HOST AGGREGATE DAILY PROPERTIES TABLE POPULATE WITH LOOPS");
}

sub HostsinClusters
{
    my $date = SqlDate(0);
    my %Env = (
        Clusters => [
                        { 
                            Hosts =>    [
                                            {     
                                                DataLastSeen => $date,
                                                InstalledMemoryMB => 12754,
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>3,
                                                                            }
                                                                        ]
                                            }
                                        ]
                        }
                    ]
    );

                
    EnvironmentBuild (\%Env);
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Host");
    $sth ->execute();
    is ($sth->fetchrow_array(),1,"TESTING CLUSTERED HOST INSERT WITHOUT LOOPS");

    $sth = $g_dbh1->prepare(" select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by HostId) As RowNumber from SavisionCloudReporter.HostProperties) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 12754, "TESTING CONFIGURING CLUSTERED HOSTS INSERT WITHOUT LOOPS ");
    
    $sth = $g_dbh1->prepare(" select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by HostId) As RowNumber from SavisionCloudReporter.vHostAggregateDaily) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 3, "TESTING CONFIGURED CLUSTERED HOST AGGREGATE DAILY PROPERTIES TABLE POPULATE WITHOUT LOOPS");
}

sub UnclusteredVMTest
{
    my  $date = SqlDate(0);
    my %Env = (
        Hosts =>    [
                        {  
                            VMs=>   [
                                        {
                                            DataLastSeen =>$date,
                                            IsRunning   =>1 ,
                                            DailyData   =>  [
                                                                {
                                                                    RuleId =>3
                                                                }
                                                            ]
                                        },
                                    ]
                        },
                    ]
    );

    EnvironmentBuild(\%Env,);
    
    my $sth = $g_dbh1->prepare("Select Count (*) From SavisionCloudReporter.VM");
    $sth -> execute();
    is ( $sth->fetchrow_array() , 1, "TESTING  UNCLUSTERED VM INSERT WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare("select DataLastSeen from  (select DataLastSeen , ROW_NUMBER() OVER (Order by VMId) As RowNumber from SavisionCloudReporter.VM) T where T.RowNumber = 1");
    $sth -> execute();
    is ($sth->fetchrow_array(), $date, "TESTING CONFIGURING UNCLUSTERED VM INSERT WITHOUT LOOPS");

    $sth = $g_dbh1->prepare("select IsRunning from  (select IsRunning , ROW_NUMBER() OVER (Order by VMPropertiesId) As RowNumber from SavisionCloudReporter.VMProperties) T where T.RowNumber = 1");
    $sth -> execute();
    is ($sth->fetchrow_array(), 1,"TESTING CONFIGURING UNCLUSTERED VM PROPERTIES INSERT WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare("select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by VMId) As RowNumber from SavisionCloudReporter.vVMAggregateDaily) T where T.RowNumber = 1");
    $sth -> execute();
    is ($sth->fetchrow_array(), 3,"TESTING CONFIGURING UNCLUSTERED VM AGGREGATEDAILY INSERT WITHOUT LOOPS");
}

sub ClusteredVMTest
{
    my  $date = SqlDate(0);
    my %Env = (
        Clusters => [
                        {
                            Hosts =>    [   
                                            {  
                                                VMs=>   [
                                                            {
                                                                DataLastSeen    =>$date,
                                                                IsRunning       =>1 ,
                                                                DailyData       =>  [
                                                                                        {
                                                                                            RuleId =>3
                                                                                        }
                                                                                    ]
                                                            },
                                                        ]
                                            },   
                                        ]
                        },
                    ]
    );

    EnvironmentBuild(\%Env);
    
    my $sth = $g_dbh1->prepare("Select Count (*) From SavisionCloudReporter.VM");
    $sth -> execute();
    is ( $sth->fetchrow_array() , 1, "TESTING  CLUSTERED VM INSERT WITHOUT LOOPS");

    $sth = $g_dbh1->prepare("select DataLastSeen from  (select DataLastSeen , ROW_NUMBER() OVER (Order by VMId) As RowNumber from SavisionCloudReporter.VM) T where T.RowNumber = 1");
    $sth -> execute();
    is ($sth->fetchrow_array(), $date, "TESTING CONFIGURING CLUSTERED VM INSERT WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare("select IsRunning from  (select IsRunning , ROW_NUMBER() OVER (Order by VMPropertiesId) As RowNumber from SavisionCloudReporter.VMProperties) T where T.RowNumber = 1");
    $sth -> execute();
    is ($sth->fetchrow_array(), 1,"TESTING CONFIGURING CLUSTERED VM PAST DATA INSERT WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare("select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by VMId) As RowNumber from SavisionCloudReporter.vVMAggregateDaily) T where T.RowNumber = 1");
    $sth -> execute();
    is ($sth->fetchrow_array(), 3,"TESTING CONFIGURING CLUSTERED VM AGGREGATEDAILY INSERT WITHOUT LOOPS");

}

sub UnclusteredVMTestLoops
{
    my  $date = SqlDate(0);
    my %Env = (
        Hosts =>    [
                        "Loop_Host1",
                        {  
                            VMs=>   [
                                        "Loop_VM1",
                                        {
                                            DataLastSeen =>$date,
                                            IsRunning   =>1 ,
                                            DailyData       =>  [
                                                                    {
                                                                        RuleId =>3
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
                Loop_Host1  =>  shift @_,
                Loop_VM1    =>  shift @_
            );
    
    EnvironmentBuild(\%Env,\%loop);
    
    my $sth = $g_dbh1->prepare("Select Count (*) From SavisionCloudReporter.VM");
    $sth -> execute();
    is ( $sth->fetchrow_array() , $loop{Loop_Host1}*$loop{Loop_VM1}, "TESTING  UNCLUSTERED VM INSERT WITH LOOPS");

    $sth = $g_dbh1->prepare("select DataLastSeen from  (select DataLastSeen , ROW_NUMBER() OVER (Order by VMId) As RowNumber from SavisionCloudReporter.VM) T where T.RowNumber = $loop{Loop_Host1}*$loop{Loop_VM1}");
    $sth -> execute();
    is ($sth->fetchrow_array(), $date, "TESTING CONFIGURING UNCLUSTERED VM INSERT WITH LOOPS");
    
    $sth = $g_dbh1->prepare("select IsRunning from  (select IsRunning , ROW_NUMBER() OVER (Order by VMPropertiesId) As RowNumber from SavisionCloudReporter.VMProperties) T where T.RowNumber = $loop{Loop_Host1}*$loop{Loop_VM1}");
    $sth -> execute();
    is ($sth->fetchrow_array(), 1,"TESTING CONFIGURING UNCLUSTERED VM PAST DATA INSERT WITH LOOPS");
    
    $sth = $g_dbh1->prepare("select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by VMId) As RowNumber from SavisionCloudReporter.vVMAggregateDaily) T where T.RowNumber = $loop{Loop_Host1}*$loop{Loop_VM1}");
    $sth -> execute();
    is ($sth->fetchrow_array(), 3,"TESTING CONFIGURING UNCLUSTERED VM AGGREGATEDAILY INSERT WITH LOOPS");
}

sub ClusteredVMTestLoops
{
    my  $date = SqlDate(0);
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        {
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {  
                                                VMs=>   [
                                                            "Loop_VM1",
                                                            {
                                                                DataLastSeen =>$date,
                                                                IsRunning   =>1 ,
                                                                DailyData       =>  [
                                                                                        {
                                                                                            RuleId =>3
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
    my %loop =  (
                Loop_Cluster1   => shift @_,
                Loop_Host1      => shift @_,    
                Loop_VM1        => shift @_,
            );
    EnvironmentBuild(\%Env,\%loop);
    
    my $sth = $g_dbh1->prepare("Select Count (*) From SavisionCloudReporter.VM");
    $sth -> execute();
    is ( $sth->fetchrow_array() , $loop{Loop_Host1}*$loop{Loop_VM1}, "TESTING  CLUSTERED VM INSERT WITH LOOPS");

    $sth = $g_dbh1->prepare("select DataLastSeen from  (select DataLastSeen , ROW_NUMBER() OVER (Order by VMId) As RowNumber from SavisionCloudReporter.VM) T where T.RowNumber = $loop{Loop_Host1}*$loop{Loop_VM1}");
    $sth -> execute();
    is ($sth->fetchrow_array(), $date, "TESTING CONFIGURING CLUSTERED VM INSERT WITH LOOPS");
    
    $sth = $g_dbh1->prepare("select IsRunning from  (select IsRunning , ROW_NUMBER() OVER (Order by VMPropertiesId) As RowNumber from SavisionCloudReporter.VMProperties) T where T.RowNumber = $loop{Loop_Host1}*$loop{Loop_VM1}");
    $sth -> execute();
    is ($sth->fetchrow_array(), 1,"TESTING CONFIGURING CLUSTERED VM PAST DATA INSERT WITH LOOPS");
    
    $sth = $g_dbh1->prepare("select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by VMId) As RowNumber from SavisionCloudReporter.vVMAggregateDaily) T where T.RowNumber = $loop{Loop_Host1}*$loop{Loop_VM1}");
    $sth -> execute();
    is ($sth->fetchrow_array(), 3,"TESTING CONFIGURING CLUSTERED VM AGGREGATEDAILY INSERT WITH LOOPS");

}

sub JustHostLoops
{
    my %Env = (
        Hosts =>    [
                        "Loop_Host1",
                        {  
                            DataLastSeen => SqlDate(0),
                            InstalledMemoryMB => 1423,
                            DailyData           =>  [
                                                        {
                                                            RuleId =>3
                                                        }
                                                    ]
                        },
                        "End_Loop"
                    ]
    );
    my %loop = (
                Loop_Host1   => @_[0]
            );
    EnvironmentBuild (\%Env,\%loop);
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Host");
    $sth ->execute();
    is ($sth->fetchrow_array(),$loop{Loop_Host1},"TESTING UNCLUSTERED HOST INSERT WITH LOOPS");
    
    $sth = $g_dbh1->prepare(" select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by HostId) As RowNumber from SavisionCloudReporter.HostProperties) T where T.RowNumber = $loop{Loop_Host1}");
    $sth ->execute();
    is ($sth->fetchrow_array(), 1423, "TESTING CONFIGURING UNCLUSTERED HOSTS INSERT WITH LOOPS ");

    $sth = $g_dbh1->prepare(" select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by HostId) As RowNumber from SavisionCloudReporter.vHostAggregateDaily) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 3, "TESTING CONFIGURED HOST AGGREGATE DAILY PROPERTIES TABLE POPULATE WITHOUT LOOPS");
}

sub JustHost
{
    my $date = SqlDate(0);
    my %Env = (
        Hosts =>    [
                        {  
                            DataLastSeen => $date,
                            InstalledMemoryMB => 1423,
                            DailyData           =>  [
                                                        {
                                                            RuleId =>3
                                                        }
                                                    ]
                        }
                    ]
    );

    EnvironmentBuild (\%Env,);
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.Host");
    $sth ->execute();
    is ($sth->fetchrow_array(),1,"TESTING UNCLUSTERED HOST INSERT WITHOUT LOOPS");

    $sth = $g_dbh1->prepare(" select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by HostId) As RowNumber from SavisionCloudReporter.HostProperties) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 1423, "TESTING CONFIGURING UNCLUSTERED HOSTS INSERT WITHOUT LOOPS ");
    
    $sth = $g_dbh1->prepare(" select RuleId from  (select RuleId , ROW_NUMBER() OVER (Order by HostId) As RowNumber from SavisionCloudReporter.vHostAggregateDaily) T where T.RowNumber = 1");
    $sth ->execute();
    is ($sth->fetchrow_array(), 3, "TESTING CONFIGURED UNCLUSTERED HOST AGGREGATE DAILY PROPERTIES TABLE POPULATE WITHOUT LOOPS");

}

sub MultiplePropertyUnclusteredHosts
{
    my %Env = (
        Hosts =>    [   
                        {  
                            DataLastSeen => SqlDate(0),
                            InstalledMemoryMB=> 2345,
                            ValidToDate => undef,
                            ValidFromDate=> SqlDate(-4),
                            PastProperties=>  [   
                                            {
                                                InstalledMemoryMB=> 2345,
                                                ValidToDate => SqlDate(-4),
                                                ValidFromDate=> SqlDate(-8),
                                            }
                                        ]
                        },
                    ]
    );

    
    EnvironmentBuild(\%Env,);
    
    my $sth = $g_dbh1->prepare("Select Count (*) From SavisionCloudReporter.HostProperties");
    $sth -> execute();
    is ( $sth->fetchrow_array, 2, "TESTING MULTIPLE PROPERTY LINE CLUSTERED HOSTS WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare("select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by HostPropertiesId) As RowNumber from SavisionCloudReporter.HostProperties) T where T.RowNumber = 2");
    $sth->execute();
    is ($sth->fetchrow_array(), 2345,"TESTING CONFIGURING CLUSTERED HOST PAST DATA WITHOUT LOOPS");
}

sub MultiplePropertyClusteredHosts
{
    my %Env = (
        Clusters => [
                        {
                        Hosts =>    [   
                                        {  
                                            DataLastSeen => SqlDate(0),
                                            InstalledMemoryMB=> 246345,
                                            ValidToDate => undef,
                                            ValidFromDate=> SqlDate(-4),
                                            PastProperties=>  [
                                                            {
                                                                InstalledMemoryMB=> 234564567,
                                                                ValidToDate => SqlDate(-4),
                                                                ValidFromDate=> SqlDate(-8),
                                                            }
                                                        ]   
                                        },
                                    ]
                        },
                    ]
    );
    EnvironmentBuild(\%Env,);
    my $sth = $g_dbh1->prepare("Select Count (*) From SavisionCloudReporter.HostProperties");
    $sth -> execute();
    is ( $sth->fetchrow_array, 2, "TESTING MULTIPLE PROPERTY LINE CLUSTERED HOSTS WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare("select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by HostPropertiesId) As RowNumber from SavisionCloudReporter.HostProperties) T where T.RowNumber = 2");
    $sth->execute();
    is ($sth->fetchrow_array(), 234564567,"TESTING CONFIGURING CLUSTERED HOST PAST DATA WITHOUT LOOPS");
}

sub MultiplePropertyUnclusteredVM
{
    my %Env = (
        Hosts =>    [
                        {  
                            DataLastSeen => SqlDate(0),
                            InstalledMemoryMB=> 2345,
                            ValidToDate => undef,
                            ValidFromDate=> SqlDate(-4),
                            VMs=>   [
                                        {
                                            
                                            IsRunning =>1 ,
                                            ValidToDate => SqlDate(-4),
                                            ValidFromDate=> SqlDate(-8),
                                            PastProperties=>  [
                                                            {
                                                                IsRunning =>0 ,
                                                                ValidToDate => undef,
                                                                ValidFromDate=> SqlDate(-4),
                                                            }
                                                        ]
                                        },
                                    ]
                        },
                    ]
    );
    my %loop = (
                Loop_Host1  =>  shift @_,
                Loop_VM1    =>  shift @_
            );
    
    EnvironmentBuild(\%Env,\%loop);
    
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.VMProperties");
    $sth -> execute();
    is ( $sth->fetchrow_array() , 2, "TESTING MULTIPLE PROPERTY LINE UNCLUSTERED VMs WITHOUT LOOPS");
    
    
    $sth = $g_dbh1->prepare("select IsRunning from  (select IsRunning , ROW_NUMBER() OVER (Order by VMPropertiesId) As RowNumber from SavisionCloudReporter.VMProperties) T where T.RowNumber = 2");
    $sth -> execute();
    is ($sth->fetchrow_array(), 0,"TESTING CONFIGURING UNCLUSTERED VM PAST DATA WITHOUT LOOPS");
}

sub MultiplePropertyClusteredVM
{
    my %Env = (
        Clusters => [
                        {
                            Hosts =>    [   
                                            {  
                                                DataLastSeen => SqlDate(0),
                                                InstalledMemoryMB=> 2345,
                                                ValidToDate => undef,
                                                ValidFromDate=> SqlDate(-4),
                                                VMs=>   [
                                                            {
                                                                IsRunning =>1 ,
                                                                ValidToDate => undef,
                                                                ValidFromDate=> SqlDate(-8),
                                                                PastProperties=>  [
                                                                                {
                                                                                    IsRunning =>0 ,
                                                                                    ValidToDate => SqlDate(-8),
                                                                                    ValidFromDate=> SqlDate(-14),
                                                                                }
                                                                            ]
                                                            },
                                                        ]
                                            },   
                                        ]
                        },
                    ]
    );
    EnvironmentBuild(\%Env);
    
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.VMProperties");
    $sth->execute();
    is ( $sth->fetchrow_array() , 2, "TESTING MULTIPLE PROPERTY LINE CLUSTERED VMs WITHOUT LOOPS");
    
    $sth = $g_dbh2->prepare("select IsRunning from  (select IsRunning , ROW_NUMBER() OVER (Order by VMPropertiesId) As RowNumber from SavisionCloudReporter.VMProperties) T where T.RowNumber = 2");
    $sth ->execute();
    is ($sth->fetchrow_array(), 0,"TESTING CONFIGURING CLUSTERED VM PAST DATA WITHOUT LOOPS");

}

sub MultiplePropertyClusters
{
    my %Env = (
        Clusters => [
                        { 
                            InstalledMemoryMB => 0,
                            ValidToDate => undef,
                            ValidFromDate => SqlDate(-4),
                            PastProperties => [
                                            {
                                                InstalledMemoryMB => 4500,
                                                ValidToDate => SqlDate(-4),
                                                ValidFromDate => SqlDate(-10),
                                            }
                                        ]
                        },
                    ]
    );

    EnvironmentBuild(\%Env,);
    
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.ClusterProperties");
    $sth ->execute();
    is ( $sth->fetchrow_array() , 2, "TESTING MULTIPLE PROPERTY LINE CLUSTERS WITHOUT LOOPS");
    
    $sth = $g_dbh2->prepare("select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by ClusterPropertiesId) As RowNumber from SavisionCloudReporter.ClusterProperties) T where T.RowNumber = 2");
    $sth ->execute();
    is ( $sth->fetchrow_array(), 4500,"TESTING CONFIGURING CLUSTER PAST DATA WITHOUT LOOPS");



}

sub MultiplePropertyClusterStorage
{
    my %Env = (
        Clusters =>     [   
                            {   
                                InstalledMemoryMB => 0,
                                ClusterStorage =>   [   
                                                        {
                                                            InstalledDiskSpaceMB => 10008,
                                                            PastProperties=>  [
                                                                            {
                                                                                InstalledDiskSpaceMB =>12345
                                                                            }
                                                                        ]
                                                        },
                                                    ],
                            },
                        ]
    );

    

    EnvironmentBuild (\%Env);

    my $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.ClusterStorageProperties");
    $sth ->execute();
    is ( $sth->fetchrow_array(), 2, "TESTING CLUSTER STORAGE INSERT WITHOUT LOOPS");
    
    $sth = $g_dbh1->prepare(" select InstalledDiskSpaceMB from  (select InstalledDiskSpaceMB , ROW_NUMBER() OVER (Order by ClusterStoragePropertiesId) As RowNumber from SavisionCloudReporter.ClusterStorageProperties) T where T.RowNumber = 2");
    $sth ->execute();
    is ( $sth->fetchrow_array(), 12345, "TESTING CONFIGURE CLUSTER STORAGE ");
}

sub MultiplePropertyUnclusteredHostLoops
{
    my %Env = (
        Hosts =>    [   
                        "Loop_Host1",
                        {  
                            DataLastSeen => SqlDate(0),
                            InstalledMemoryMB=> 2345,
                            ValidToDate => undef,
                            ValidFromDate=> SqlDate(-4),
                            PastProperties=>  [   
                                            {
                                                InstalledMemoryMB=> 2345,
                                                ValidToDate => SqlDate(-4),
                                                ValidFromDate=> SqlDate(-8),
                                            }
                                        ]
                        },
                        "End_Loop"
                    ]
    );
    my %loop = (
                    Loop_Host1=>@_[0]
                );
    
    EnvironmentBuild(\%Env,\%loop);
    
    my $sth = $g_dbh1->prepare("Select Count (*) From SavisionCloudReporter.HostProperties");
    $sth -> execute();
    is ( $sth->fetchrow_array, 2*$loop{Loop_Host1}, "TESTING MULTIPLE PROPERTY LINE CLUSTERED HOSTS WITH LOOPS");
    
    $sth = $g_dbh1->prepare("select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by HostPropertiesId) As RowNumber from SavisionCloudReporter.HostProperties) T where T.RowNumber = 2");
    $sth->execute();
    is ($sth->fetchrow_array(), 2345,"TESTING CONFIGURING CLUSTERED HOST PAST DATA WITH LOOPS");
}

sub MultiplePropertyClusteredHostLoops
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        {
                        Hosts =>    [   
                                        "Loop_Host1",
                                        {  
                                            DataLastSeen => SqlDate(0),
                                            InstalledMemoryMB=> 246345,
                                            ValidToDate => undef,
                                            ValidFromDate=> SqlDate(-4),
                                            PastProperties=>  [
                                                            {
                                                                InstalledMemoryMB=> 234564567,
                                                                ValidToDate => SqlDate(-4),
                                                                ValidFromDate=> SqlDate(-8),
                                                            }
                                                        ]   
                                        },
                                        "End_Loop"
                                    ]
                        },
                        "End_Loop"
                    ]
    );
    my %loop =  (
                    Loop_Cluster1 => @_[0],
                    Loop_Host1 => @_[1]
                );
    EnvironmentBuild(\%Env,\%loop);
    my $sth = $g_dbh1->prepare("Select Count (*) From SavisionCloudReporter.HostProperties");
    $sth -> execute();
    is ( $sth->fetchrow_array, 2*$loop{Loop_Cluster1}*$loop{Loop_Host1}, "TESTING MULTIPLE PROPERTY LINE CLUSTERED HOSTS WITH LOOPS");
    
    $sth = $g_dbh1->prepare("select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by HostPropertiesId) As RowNumber from SavisionCloudReporter.HostProperties) T where T.RowNumber = 2");
    $sth->execute();
    is ($sth->fetchrow_array(), 234564567,"TESTING CONFIGURING CLUSTERED HOST PAST DATA WITH LOOPS");
}

sub MultiplePropertyUnclusteredVMLoops
{
    my %Env = (
        Hosts =>    [
                        "Loop_Host1",
                        {  
                            DataLastSeen => SqlDate(0),
                            InstalledMemoryMB=> 2345,
                            ValidToDate => undef,
                            ValidFromDate=> SqlDate(-4),
                            VMs=>   [
                                        "Loop_VM1",
                                        {
                                            
                                            IsRunning =>1 ,
                                            ValidToDate => SqlDate(-4),
                                            ValidFromDate=> SqlDate(-8),
                                            PastProperties=>  [
                                                            {
                                                                IsRunning =>0 ,
                                                                ValidToDate => undef,
                                                                ValidFromDate=> SqlDate(-4),
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
                Loop_Host1  =>  shift @_,
                Loop_VM1    =>  shift @_
            );
    
    EnvironmentBuild(\%Env,\%loop);
    
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.VMProperties");
    $sth -> execute();
    is ( $sth->fetchrow_array() , 2*$loop{Loop_Host1}*$loop{Loop_VM1}, "TESTING MULTIPLE PROPERTY LINE UNCLUSTERED VMs WITH LOOPS");
    
    
    $sth = $g_dbh1->prepare("select IsRunning from  (select IsRunning , ROW_NUMBER() OVER (Order by VMPropertiesId) As RowNumber from SavisionCloudReporter.VMProperties) T where T.RowNumber = 2");
    $sth -> execute();
    is ($sth->fetchrow_array(), 0,"TESTING CONFIGURING UNCLUSTERED VM PAST DATA WITH LOOPS");
}

sub MultiplePropertyClusteredVMLoops
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        {
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {  
                                                DataLastSeen => SqlDate(0),
                                                InstalledMemoryMB=> 2345,
                                                ValidToDate => undef,
                                                ValidFromDate=> SqlDate(-4),
                                                VMs=>   [
                                                            "Loop_VM1",
                                                            {
                                                                IsRunning =>1 ,
                                                                ValidToDate => undef,
                                                                ValidFromDate=> SqlDate(-8),
                                                                PastProperties=>  [
                                                                                {
                                                                                    IsRunning =>0 ,
                                                                                    ValidToDate => SqlDate(-8),
                                                                                    ValidFromDate=> SqlDate(-14),
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
    my %loop =  (
                Loop_Cluster1   => shift @_,
                Loop_Host1      => shift @_,    
                Loop_VM1        => shift @_,
            );
    EnvironmentBuild(\%Env,\%loop);
    
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.VMProperties");
    $sth->execute();
    is ( $sth->fetchrow_array() , 2*$loop{Loop_Cluster1}*$loop{Loop_Host1}*$loop{Loop_VM1}, "TESTING MULTIPLE PROPERTY LINE CLUSTERED VMs WITH LOOPS");
    
    $sth = $g_dbh2->prepare("select IsRunning from  (select IsRunning , ROW_NUMBER() OVER (Order by VMPropertiesId) As RowNumber from SavisionCloudReporter.VMProperties) T where T.RowNumber = 2");
    $sth ->execute();
    is ($sth->fetchrow_array(), 0,"TESTING CONFIGURING CLUSTERED VM PAST DATA WITH LOOPS");

}

sub MultiplePropertyClustersLoops
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            InstalledMemoryMB => 0,
                            ValidToDate => undef,
                            ValidFromDate => SqlDate(-4),
                            PastProperties => [
                                            {
                                                InstalledMemoryMB => 4500,
                                                ValidToDate => SqlDate(-4),
                                                ValidFromDate => SqlDate(-10),
                                            }
                                        ]
                        },
                        "End_Loop"
                    ]
    );
    
    my %loop =  (
            Loop_Cluster1   => shift @_,
        );
    EnvironmentBuild(\%Env,\%loop);
    
    my $sth = $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.ClusterProperties");
    $sth ->execute();
    is ( $sth->fetchrow_array() , 2*$loop{Loop_Cluster1}, "TESTING MULTIPLE PROPERTY LINE CLUSTERS WITH LOOPS");
    
    $sth = $g_dbh2->prepare("select InstalledMemoryMB from  (select InstalledMemoryMB , ROW_NUMBER() OVER (Order by ClusterPropertiesId) As RowNumber from SavisionCloudReporter.ClusterProperties) T where T.RowNumber = 2");
    $sth ->execute();
    is ( $sth->fetchrow_array(), 4500,"TESTING CONFIGURING CLUSTER PAST DATA WITH LOOPS");



}

sub MultiplePropertyClusterStorageLoops
{
    my %Env = (
        Clusters =>     [   
                            "Loop_Cluster1",
                            {   
                                InstalledMemoryMB => 0,
                                ClusterStorage =>   [   
                                                        "Loop_Storage1",
                                                        {
                                                            InstalledDiskSpaceMB => 10008,
                                                            PastProperties=>  [
                                                                            {
                                                                                InstalledDiskSpaceMB =>12345
                                                                            }
                                                                        ]
                                                        },
                                                        "End_Loop"
                                                    ],
                            },
                            "End_Loop"
                        ]
    );

    
    my %loop =  (
                    Loop_Cluster1   => @_[0],
                    Loop_Storage1   => @_[1],
                );
    EnvironmentBuild (\%Env,\%loop);

    my $sth =  $g_dbh1->prepare(" Select Count (*) From SavisionCloudReporter.ClusterStorageProperties");
    $sth ->execute();
    is ( $sth->fetchrow_array(), 2*$loop{Loop_Cluster1}*$loop{Loop_Storage1}, "TESTING CLUSTER STORAGE INSERT WITH LOOPS");
    
    $sth = $g_dbh1->prepare(" select InstalledDiskSpaceMB from  (select InstalledDiskSpaceMB , ROW_NUMBER() OVER (Order by ClusterStoragePropertiesId) As RowNumber from SavisionCloudReporter.ClusterStorageProperties) T where T.RowNumber = 2");
    $sth ->execute();
    is ( $sth->fetchrow_array(), 12345, "TESTING CONFIGURE CLUSTER STORAGE WITH LOOPS");
}
