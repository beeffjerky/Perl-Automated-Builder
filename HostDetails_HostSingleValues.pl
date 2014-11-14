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
                    
use constant TEST_QUERY         =>  'exec Savision_CloudReporter_Report_HyperV_HostDetails_HostSingleValues
                                    @HostId = ?'
;
use constant DSN =>'dbi:ODBC:CloudReporterTest';

my $connection = DBIx::Connection->new(name => 'test', dsn => DSN, username => 'HEROES\mike', password => 'P@ssw0rd');
my $g_dbh1 = DBI->connect(DSN);
my $g_dbh2 = DBI->connect(DSN);


=pod
EmptyTables();
NonRecentDailyData(1,1);
ClusteredHostsWithRelevantData(2,1);
UnclusteredHostsWithRelevantData(2);
=cut
#MultipleDataPerDay(1,1);
ClusteredHostsNoValueData(1,1);
UnclusteredHostsNoValueData(1);

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
    $sth->execute($value);
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
                                                ValidToDate=>undef,
                                                InstalledProcessorMHz=>1024,
                                                InstalledLocalStorageMB=>1234,
                                                AvailiblePhysicalRamInMB=>2345,
                                                NumberOfLogicalCPUs =>4,
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
                                                                                RuleId =>14,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-11)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>18,
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
        $sth->execute(HostIdFromEntity($number));
        my $rows = 0;
        while( $sth->fetchrow_arrayref) {
            $rows++;
        }
        is ($rows,0,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH DATA OUT OF DATE BY ONE DAY");
    }
}

sub ClusteredHostsWithRelevantData
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                InstalledMemoryMB => 12754,
                                                ValidToDate=>undef,
                                                InstalledProcessorMHz=>1024,
                                                InstalledLocalStorageMB=>1234,
                                                AvailiblePhysicalRamInMB=>2345,
                                                NumberOfLogicalCPUs =>4,
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>14,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>18,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1)                                                              
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1) 
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
        $sth->execute(HostIdFromEntity($number));
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS WITH FILLED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number));
        $row = $sth->fetchrow_arrayref;
        is(@{$row}[1],"Host-$number","TESTING HostName VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        
        my $value_sth = $g_dbh2 -> prepare("Select Top 1 ClusterId from SavisionCloudReporter.HostProperties where HostId =  ? ORDER BY HostPropertiesId DESC ");
        $value_sth->execute(@{$row}[0]);
        my $clusterId = $value_sth->fetchrow_array;
        is(@{$row}[2], $clusterId,"TESTING ClusterId VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        
        $value_sth = $g_dbh2 -> prepare("Select Top 1 ClusterName from SavisionCloudReporter.Cluster where ClusterId =  ? ORDER BY ClusterId DESC ");
        $value_sth->execute($clusterId);
        is(@{$row}[3], $value_sth->fetchrow_array,"TESTING ClusterName VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        
        

        is(@{$row}[4],  1,"TESTING IsClustered VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[5], SqlDate(-1),"TESTING DataFirstSeen VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[6], SqlDate(0),"TESTING DataLastSeen VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[7], 12754,"TESTING InstalledMemeoryMb VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[8],1024,"TESTING InstalledProcessorMHz VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[9],1234,"TESTING InstalledLOCALSTORAGE VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[10],4,"TESTING NumberOfLOGICALCPUs VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[11],"2048.0","TESTING AvailiblePhysicalRamInMb VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[12],"2.0","TESTING NumberOfConfiguredVirtualMachines VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[13],"2.0","TESTING NumberOfRunningVirtualMachines VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[14],"2.0","TESTING NumberOfVirtualCpus VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[15],"2048.0","TESTING AvailibleLocalDiskSpaceInMb VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
        is(@{$row}[16],"1024.0","TESTING TotalProcessorIdleMHZ VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} UNCLUSTERED HOSTS");
    }
}

sub UnclusteredHostsWithRelevantData
{
    my %Env = (
        Hosts =>    [   
                        "Loop_Host1",
                        {     
                            InstalledMemoryMB => 12754,
                            ValidToDate=>undef,
                            InstalledProcessorMHz=>1024,
                            InstalledLocalStorageMB=>1234,
                            AvailiblePhysicalRamInMB=>2345,
                            NumberOfLogicalCPUs =>4,
                            DailyData           =>  [
                                                        {
                                                            RuleId =>9,
                                                            LastValue=> 2048,
                                                            Percentile_5=> 1048,
                                                            Percentile_50 => 2048,
                                                            DateTime=>SqlDate(-1)                                                               
                                                        },
                                                        {
                                                            RuleId =>17,
                                                            LastValue=> 2,
                                                            Percentile_5=>0,
                                                            Percentile_50=>3,
                                                            DateTime=>SqlDate(-1)                                                               
                                                        },
                                                        {
                                                            RuleId =>14,
                                                            LastValue=> 2,
                                                            Percentile_5=>0,
                                                            Percentile_50=>3,
                                                            DateTime=>SqlDate(-1)                                                               
                                                        },
                                                        {
                                                            RuleId =>18,
                                                            LastValue=> 2,
                                                            Percentile_5=>0,
                                                            Percentile_50=>3,
                                                            DateTime=>SqlDate(-1)                                                               
                                                        },
                                                        {
                                                            RuleId =>8,
                                                            LastValue=> 2048,
                                                            Percentile_5=> 1048,
                                                            Percentile_50 => 2048,
                                                                                                                          
                                                        },
                                                        {
                                                            RuleId =>21,
                                                            LastValue=> 2048,
                                                            Percentile_5=> 1024,
                                                            Percentile_50 => 2048,
                                                            DateTime=>SqlDate(-1) 
                                                        }
                                                    ]
                        },
                        "End_Loop"
                    ]

                    
    );        
    my %loop = (
        Loop_Host1      =>shift @_,
        );
    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number));
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Host1} CLUSTERED HOSTS WITH FILLED DAILY DATA");
        
        $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number));
        $row = $sth->fetchrow_arrayref;
        is(@{$row}[1],"Host-$number","TESTING HostName VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");        
        is(@{$row}[2], undef,"TESTING ClusterId VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");    
        is(@{$row}[3], undef,"TESTING ClusterName VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[4],  0,"TESTING IsClustered VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[5], SqlDate(-1),"TESTING DataFirstSeen VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[6], SqlDate(0),"TESTING DataLastSeen VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[7], 12754,"TESTING InstalledMemeoryMb VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[8],1024,"TESTING InstalledProcessorMHz VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[9],1234,"TESTING InstalledLOCALSTORAGE VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[10],4,"TESTING NumberOfLOGICALCPUs VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[11],"2048.0","TESTING AvailiblePhysicalRamInMb VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[12],"2.0","TESTING NumberOfConfiguredVirtualMachines VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[13],"2.0","TESTING NumberOfRunningVirtualMachines VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[14],"2.0","TESTING NumberOfVirtualCpus VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[15],"2048.0","TESTING AvailibleLocalDiskSpaceInMb VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
        is(@{$row}[16],"1024.0","TESTING TotalProcessorIdleMHZ VALUE FROM $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS");
    }
}

sub MultipleDataPerDay
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                InstalledMemoryMB => 12754,
                                                ValidToDate=>undef,
                                                InstalledProcessorMHz=>1024,
                                                InstalledLocalStorageMB=>1234,
                                                AvailiblePhysicalRamInMB=>2345,
                                                NumberOfLogicalCPUs =>4,
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>14,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>18,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1,30)                                                              
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1) 
                                                                            },
                                                                            {
                                                                                RuleId =>9,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1,3)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1,3)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>14,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1,3)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>18,
                                                                                LastValue=> 2,
                                                                                Percentile_5=>0,
                                                                                Percentile_50=>3,
                                                                                DateTime=>SqlDate(-1,3)                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1048,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1,3)
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                LastValue=> 2048,
                                                                                Percentile_5=> 1024,
                                                                                Percentile_50 => 2048,
                                                                                DateTime=>SqlDate(-1,3) 
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
  


}

sub ClusteredHostsNoValueData
{
    my %Env = (
        Clusters => [
                        "Loop_Cluster1",
                        { 
                            Hosts =>    [   
                                            "Loop_Host1",
                                            {     
                                                InstalledMemoryMB => 12754,
                                                ValidToDate=>undef,
                                                InstalledProcessorMHz=>1024,
                                                InstalledLocalStorageMB=>1234,
                                                AvailiblePhysicalRamInMB=>2345,
                                                NumberOfLogicalCPUs =>4,
                                                DailyData           =>  [
                                                                            {
                                                                                RuleId =>9,
                                                                                SqlDate(-1)
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>17,
                                                                                SqlDate(-1)
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>8,
                                                                                SqlDate(-1)
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>18,
                                                                                SqlDate(-1)
                                                                               
                                                                            },
                                                                            {
                                                                                RuleId =>21,
                                                                                SqlDate(-1)
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
        $sth->execute(HostIdFromEntity($number));
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Cluster1}*$loop{Loop_Host1} CLUSTERED HOSTS WITH UNPOPULATED DAILY DATA");
        
    }
}

sub UnclusteredHostsNoValueData
{
    my %Env = (
        Hosts =>    [   
                        "Loop_Host1",
                        {     
                            InstalledMemoryMB => 12754,
                            ValidToDate=>undef,
                            InstalledProcessorMHz=>1024,
                            InstalledLocalStorageMB=>1234,
                            AvailiblePhysicalRamInMB=>2345,
                            NumberOfLogicalCPUs =>4,
                            DailyData           =>  [
                                                        {
                                                            RuleId =>9,
                                                            SqlDate(-1)
                                                           
                                                        },
                                                        {
                                                            RuleId =>17,
                                                            SqlDate(-1)
                                                           
                                                        },
                                                        {
                                                            RuleId =>8,
                                                            SqlDate(-1)
                                                           
                                                        },
                                                        {
                                                            RuleId =>18,
                                                            SqlDate(-1)
                                                           
                                                        },
                                                        {
                                                            RuleId =>21,
                                                            SqlDate(-1)
                                                        }
                                                    ]
                        },
                        "End_Loop"
                    ]             
    );        
    my %loop = (
        Loop_Host1      =>shift @_,
        );
    EnvironmentBuild (\%Env,\%loop);
    for( my $number = 1; $number != $loop{Loop_Host1}+1;$number++ ){
        my $sth = $g_dbh1 -> prepare( TEST_QUERY);
        $sth->execute(HostIdFromEntity($number));
        my $rows = 0;
        my $row;
        while( $row = $sth->fetchrow_arrayref) {
            $rows++;
        }
        
        is ($rows,1,"TESTING $number OUT OF $loop{Loop_Host1} UNCLUSTERED HOSTS WITH UNPOPULATED DAILY DATA");
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
    my $sth = $g_dbh2->prepare ("Select Top 1 HostId from SavisionCloudReporter.HostManagedEntityMap Where ManagedEntityRowId = ? ORDER BY ManagedEntityRowId DESC ");
    $sth->execute($managedId);
    my $hostId = $sth->fetchrow_array;
    $sth = undef;
    return $hostId;
}





