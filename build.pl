
use File::Path;
use File::chdir;
use File::stat;
use File::Copy;
use strict;
use Getopt::Long;
use Cwd;
use Class::Struct;
use warnings ;
use Path::Class;
use Data::Dumper ;
use File::Find::Rule ;
use POSIX ();
use POSIX qw(strftime);
use Win32::OLE qw(in with);
use Win32::OLE::Const 'Microsoft Word';
use Net::SMTP;
use Email::Send;
use Email::Send::Gmail;
use Email::Simple::Creator;

# Constant for the TFS address and collection name
use constant TFS_COLLECTION     => 'http://192.168.59.12:8080/tfs/defaultcollection';

# Constant for the temporary workspace name and owner SWD:Can we make the workspace name a GUID?
use constant TEMP_WORKSPACE     => "559FA1F1-B9FF-4D1F-A471-DDE4596A1C0D;Savision\\Mike";

# Constant storing location of TF.EXE for retrieving files and managing workspaces 
use constant TFEXE_PATH         => "\\Microsoft Visual Studio 12.0\\Common7\\IDE\\tf.exe";

# Constant storing location of DEVENV.EXE for building solutions
use constant DEVENV_PATH        => "\\Microsoft Visual Studio 12.0\\Common7\\IDE\\devenv.exe";

# Constant storing location of VTEST.CONSOL.EXE for testing builds
use constant VTEST_PATH         => "\\Microsoft Visual Studio 12.0\\Common7\\IDE\\CommonExtensions\\Microsoft\\TestWindow\\vstest.console.exe";

# Constant storing location of CHMBuilder.exe for building documentation
use constant DOCBUILD_PATH      =>"\\EWSoftware\\Sandcastle Help File Builder\\ChmBuilder.exe";

# Constant for log in credentials
use constant LOG_CREDS          => 'Savision\mike,S@vision';

# Constant for path storing past builds
use constant PAST_BUILDS        => "C:\\Past Builds\\";

# Constant for rasdial commands
use constant CONNECTION         => "rasdial".' "Savision Amsterdam" Mike S@vision';

# Structure defining Savision Product Build Parameters


struct SavisionProductBuild =>
    [
        SolutionFile     => '$',
        BuildFile        => '$',
        TestFile         => '$',
        VersionFile      => '$',
        UpdateFiles      => '@',
        TFSGetPath       => '$',
        TFSCheckOut      => '@',
        TFSCheckIn       => '@',
        PostBuildFile    => '$',
        UserNotes        => '@',
        HelpFile         => '$',

    ];
my $CloudAdv = SavisionProductBuild -> new( SolutionFile    =>"\\Vital Signs For VI\\Cloud Advisor\\Cloud Advisor VMM Console Add-Ins\\Cloud Advisor VMM Console Add-Ins.sln",
                                            BuildFile       =>"\\vital signs For VI\\Cloud Advisor\\Cloud Advisor VMM Console Add-Ins\\bin\\Debug\\Savision.CloudAdvisor.VMMConsoleAddIns.TuningTips.dll",
                                            TestFile        =>"\\vital signs for VI\\Cloud Advisor\\Test Cloud Advisor VMM Console Add-Ins\\bin\\Debug\\Test Savision Vital Signs VMM Console Add-Ins.dll",
                                            VersionFile     =>"\\vital signs For VI\\Cloud Advisor\\Cloud Advisor VMM Console Add-Ins\\MASTER_VERSION.txt",
                                            UpdateFiles     =>["\\vital signs For VI\\Cloud Advisor\\Cloud Advisor VMM Console Add-Ins\\build\\Add-In Manifest\\Manifest.xml",
                                                               "\\vital signs For VI\\Cloud Advisor\\Cloud Advisor VMM Console Add-Ins\\Properties\\AssemblyInfo.cs"],
                                            TFSGetPath      =>'"$/vital signs for VI/Cloud Advisor"',
                                            TFSCheckOut     =>['"$/vital signs for VI/Cloud Advisor/Cloud Advisor VMM console Add-Ins/MASTER_VERSION.txt"',
                                                               '"$/vital signs for VI/Cloud Advisor/Cloud Advisor VMM console Add-Ins/Properties/AssemblyInfo.cs"',
                                                               '"$/vital signs for VI/Cloud Advisor/Cloud Advisor VMM console Add-Ins/build/Add-In Manifest/Manifest.xml"',
                                                               '"$/vital signs For VI/Cloud Advisor/Cloud Advisor VMM Console Add-Ins/Documentation/Cloud_Advisor_Release_Notes.docx"',
                                                               '"$/vital signs For VI/Cloud Advisor/Cloud Advisor VMM Console Add-Ins/Documentation/Cloud_Advisor_Install_Guide.docx"'],
                                            TFSCheckIn      =>['"$/vital signs for VI/Cloud Advisor/Cloud Advisor VMM console Add-Ins/MASTER_VERSION.txt"'],
                                            PostBuildFile   =>"\\vital signs For VI\\Cloud Advisor\\Cloud Advisor VMM Console Add-Ins\\bin\\Debug\\Savision Cloud Advisor.zip",
                                            UserNotes       =>["\\vital signs For VI\\Cloud Advisor\\Cloud Advisor VMM Console Add-Ins\\Documentation\\Cloud_Advisor_Release_Notes.docx",
                                                               "\\vital signs For VI\\Cloud Advisor\\Cloud Advisor VMM Console Add-Ins\\Documentation\\Cloud_Advisor_Install_Guide.docx"],
                                            HelpFile        =>"\\vital signs for VI\\Cloud Advisor\\Cloud Advisor Documentation\\Help\\Documentation.chm"
                                            
                                        );

my $workingProd = $CloudAdv;

#SWD: Please prefix all globals with g_
#SWD: Prefix all options with g_opt_

# Is true if help option is passed from command line
my $g_opt_help=0;

# Force option to use path that already exists
my $g_opt_force;

# Path variable for specified working directory
my $g_opt_path;

# Command line string used to build solution at specified workspace directory.
my $g_buildCMD;

# Command line string used to test completed builds
my $g_testCMD;

# Option to get tfs build with certain label
my $g_opt_past = undef;

# Option to not check in anything
my $g_opt_test;

# Option not to delete anything
my $g_opt_nodel;

# Option not to edit any files
my $g_opt_nover;

# Option not to build any files
my $g_opt_nobuild; 

# Option determining how many files are stored
my $g_opt_storage;

# Option determining where the code documentation is located
my $g_opt_codoc;

# Global for program files location
my $g_programs;

#SWD: Can some of these be wrapped up in structures and/or made not global?

# Series of variables taken from Version.txt
my $g_opt_verNum = 0;
my $g_majorRelease;
my $g_minorRelease;
my $g_versionNumber;
my $g_buildNumber;
my $g_spec_ver;

# Global Email handler/mailer
my $g_mailer = Email::Send->new( {
    mailer => 'SMTP::TLS',
    mailer_args => [
        Host => 'smtp.gmail.com',
        Port => 587,
        User => 'vitalsignsissues@gmail.com',
        Password => 'InC0ntr0l',
        Hello => 'fayland.org',
    ]
} );


# Arrays used to organize past build storage
my @g_dates;

my @documents;
my $datetime = POSIX::strftime( "%Y%m%d", gmtime());  #SWD: name of str?


#Execution of various parts
#SWD: List all main functions here instead of chaining from one to another
SanityInput();
EnvironmentCheck();

# --nodelete option switch
unless ($g_opt_nodel){
    ConnectionUpkeep();
    DeleteWorkspace();
    ClearWorkFolder();
    CreateWorkspace();
    
    if( $g_opt_past){
        ConnectionUpkeep();
        GetLabel();
        VariableSanity();
        BuildSolution();
        ConnectionUpkeep();
        CheckBuild();
        exit;
    }
    else {
        ConnectionUpkeep();
        GetSolution();
    }
}
VariableSanity();

# Switches determining where to start build
UpdateVersion() unless $g_opt_nover;
BuildSolution() unless $g_opt_nobuild;
ConnectionUpkeep();
CheckBuild() unless $g_opt_nobuild;
Uncheckout();


    



#
#   PURPOSE:    Parses and checks the command line arguments to the script
#               Sets various variables by either grabbing them from files or from TFS
#  
#   INPUT:      Arguments are read from @ARGV
#   
#   RETURNS:    None
#
#   NOTES:
#     Script exits if help or improper arguments are detected.
#
sub SanityInput
{
    GetOptions ("path=s"        => \$g_opt_path,
                'help|h|?'      => \$g_opt_help,
                'version=s'     => \$g_opt_verNum,
                'force'         => \$g_opt_force,
                "past=s"        => \$g_opt_past,
                "test"          => \$g_opt_test,
                "nodelete"      =>  \$g_opt_nodel,
                "noversion"     =>   \$g_opt_nover,
                "nobuild"       =>  \$g_opt_nobuild,
                "storage=i"     => \$g_opt_storage,
                "codedoc=s"     =>  \$g_opt_codoc
                );
    if($g_opt_help){
        Usage();
        exit;
    }
    my $volume;
    my $directory;
    my $files;
    if(!$g_opt_path){
        print('Must use "--path <path>" to specify temporary local working directory. For help use the option --help, --h, or --?');
        exit;
    }
    # Checking validity of --path<string> option
    $g_opt_path = File::Spec->rel2abs($g_opt_path);
    
    ($volume,$directory,$files) = File::Spec->splitpath($g_opt_path, 1 );
    
    
    unless (-e $volume){
        print "$volume\\ does not exist as a drive in --path specs";
        exit;
    }
    if (-e $volume.$directory){
        print "WARNING $volume$directory will be deleted if not forced \n";
        unless ($g_opt_force){
            exit;
        }
    }
    
# Checking validity of --codedoc<string> option
    if($g_opt_codoc){
    
        $g_opt_codoc = File::Spec->rel2abs($g_opt_codoc);
        ($volume,$directory,$files) = File::Spec->splitpath($g_opt_path, 1 );
        
        unless (-e $volume){
            print "$volume\\ does not exist as a drive in --codedoc specification";
            exit;
        }
        
        unless (-e $volume.$directory){
            print "$volume$directory will be created for code documentation\n"; #SWD: think on this

        }
    }
    if ($g_opt_nodel && $g_opt_past){
        print "Cannot specify --nodelete and --past together";
        exit;
    }
}

sub ConnectionUpkeep
{
    system(CONNECTION); 
}


sub VariableSanity
{                
    $g_buildCMD = '"'.$g_programs.DEVENV_PATH.'"'." ".'"'.$g_opt_path.$workingProd->SolutionFile.'"'." /build debug /out ".$g_opt_path."\\log.txt";
    $g_testCMD = '"'.$g_programs.VTEST_PATH.'"'." ".'"'.$g_opt_path.$workingProd->TestFile.'"'." /InIsolation /Logger:trx";

# Past Build inventory check and --storage<int> warning check
    opendir my $dh, PAST_BUILDS or die "$0: opendir: $!";
    my $counter = 0;
    while (defined(my $name = readdir $dh)) {
        if ($name =~ m/Cloud Advisor (\d+).(\d+).(\d+).(\d+)_(\d+)/){
            push(@g_dates,$5);
            next;
        }
        else{
            next;  
        }
    }
    @g_dates = sort {$b <=> $a} @g_dates;
    closedir $dh;
    unless($g_opt_storage){
        $g_opt_storage = @g_dates;
    }
    else{
        if ($g_opt_storage < @g_dates){
    
            print "\n More than one old build will be deleted \n";
        }
    }
    
# Grabs current version numbers from source file
    open my $Text,  $g_opt_path.$workingProd->VersionFile or die "Cannot open $g_opt_path.$workingProd->VersionFile :$!";
    while (<$Text>){
            chomp;
            if (m/(\d+).(\d+).(\d+).(\d+)/){
                print "\n \n";
                print "Current Major Release:    $1 \n";
                print "Current Minor Release:    $2 \n";
                print "Current Revision Number:  $3 \n";
                $g_majorRelease = $1;
                $g_minorRelease = $2;
                $g_versionNumber = $3;
            }
        }
    close $Text;
# Grabs Build Number using most recent TFS changeset number for project
    my $cmd = '"'.$g_programs.TFEXE_PATH.'" history '.'"$/vital signs for VI/Cloud Advisor"'.' /noprompt /recursive /stopafter:1 /login:'.LOG_CREDS;
    my $output = qx($cmd);

    if ($output =~ m/\n(\d+)/){
        $g_buildNumber = $1;
        print "Current Build Number:     $1 \n\n";
    }
    
    if($g_opt_verNum =~ m/(\D+)(\d+).(\d+).(\d+).(\d+)(\D+)/){
        print "Version Number option must be in form <MajorRelease>.<MajorRelease>.<Revision>.<Build> no other characters may be present";
        exit;
    }

    
    if($g_opt_verNum =~ m/(\d+).(\d+).(\d+).(\d+)/){
        $g_opt_verNum = "$1.$2.$3.$4";
        print "\nSpecified Major Release:    $1 \n";
        print "Specified Minor Release:    $2 \n";
        print "Specified Revision Number:  $3 \n";
        print "Specified Build Number:     $4 \n";
        $g_spec_ver = 1;
    }
    else{
        $g_opt_verNum = "$g_majorRelease.$g_minorRelease.$g_versionNumber.$g_buildNumber";
    }
}

#
#   PURPOSE:    Checks for TF.EXE, DEVENV.EXE and 
#  
#   INPUT:      $g_opt_path               User specified temporary workspace directory
#   
#   RETURNS:    None
#
#   NOTES:
#
sub EnvironmentCheck
{
    $g_programs = qx("echo %programfiles(x86)%");
    chomp($g_programs);
    unless(-e $g_programs.DEVENV_PATH){
        print "No install of Visual Studio 2013 was found containing DEVENV.EXE on main drive \n";
        print "Please install or repair Visual Studio 2013 to $g_programs \n";
        exit;
    }
    
    unless(-e $g_programs.TFEXE_PATH){
        print "No install of Visual Studio 2013 was found containing TF.EXE on main drive \n";
        print "Please install or repair Visual Studio 2013 to $g_programs \n";
        exit;
    }
    
    unless(-e $g_programs.VTEST_PATH){
        print "No install of Visual Studio 2013 was found containing Vstest.Console.exe on main drive \n";
        print "Please install or repair Visual Studio 2013 to $g_programs \n";
        exit;
    }
    
    unless (-e $g_programs.DOCBUILD_PATH ){
        print "No install of Sandcastle Help File Builder was found \n";
        print "Please install Sandcastle Help File Builder to $g_programs \n";
    
    }
}

#
#   PURPOSE:    Deletes $g_opt_path directory and remakes it to ensure that it is present and cleared.
#  
#   INPUT:      $g_opt_path               User specified temporary workspace directory
#   
#   RETURNS:    None
#
#   NOTES:
#
sub ClearWorkFolder
{
    rmtree("$g_opt_path");
    system("mkdir $g_opt_path");
    unless (-e PAST_BUILDS)
    {
        system("mkdir ".PAST_BUILDS);
    }
    system("echo. 2>$g_opt_path\\output.txt");
}

#
#   PURPOSE:    Creates temporary workspace for TFS in visual studio using account log in
#  
#   INPUT:      $g_opt_path               User specified temporary workspace directory
#   
#   RETURNS:    None
#
#   NOTES:
#
sub CreateWorkspace
{
    local $CWD = $g_opt_path;
    print "\n";
    system('"'.$g_programs.TFEXE_PATH.'"'." workspace /new /noprompt /collection:".TFS_COLLECTION." /login:".LOG_CREDS." ".TEMP_WORKSPACE);
}

#
#   PURPOSE:    Temporary changes working directory to call TF get for the newest project version
#  
#   INPUT:      $g_opt_path               User specified temporary workspace directory
#   
#   RETURNS:    None
#
#   NOTES:
#
sub GetSolution
{
    unless($g_opt_nodel){
        local $CWD = $g_opt_path;
        system('"'.$g_programs.TFEXE_PATH.'"'." get ".$workingProd->TFSGetPath." /login:".LOG_CREDS." /recursive /force ");
        foreach(@{$workingProd->TFSCheckOut}){
            system('"'.$g_programs.TFEXE_PATH.'"'." checkout ".$_." /login:".LOG_CREDS);
        }
    }
}

#
#   PURPOSE:    Builds solution taken from TFS
#  
#   INPUT:      $g_buildCMD           The command calling devenv.exe to build the solution
#   
#   RETURNS:    None
#
#   NOTES:
#
sub BuildSolution
{ 
    unlink $g_opt_path."\\Log.txt";
    system("echo. 2>$g_opt_path\\Log.txt");
    system($g_buildCMD);
}

#
#   PURPOSE:    Determines if any errors occurred during build process
#  
#   INPUT:      $g_opt_path               User specified temporary workspace directory     
#   
#   RETURNS:    Upon pass: nothing, Upon Fail: calls method to display errors
#
#   NOTES:      
#
sub CheckBuild
{
    
    open FILE, "$g_opt_path\\log.txt" or die "Cannot open $g_opt_path\\log.txt read :$!";

    while (<FILE>) {
        chomp;
        
        if (m/======= Build: (\d+) succeeded, (\d+) failed, (\d+) up-to-date, (\d+) skipped ======/g){
            if ($2 > 0 ) {
                print ("\n \n  A build has failed or was skipped \n \n ");
                DisplayBuildError();
            }
            else{
                TestBuild();
                OldBuilds();
            }
        }  
    }
    close FILE;
}

#
#   PURPOSE:    Tests the build using the test project in each solution
#  
#   INPUT:      $g_opt_path      
#   
#   RETURNS:    OUTPUTS failed tests and counts of failed, passed, skipped 
#               and total tests
#
#   NOTES:      
#
sub TestBuild
{
    my $output = qx($g_testCMD);
    my $failedMods;
    open my $OUTPUT, '>', "$g_opt_path\\output.txt" or die "Couldn't open output.txt: $!\n";
    print $OUTPUT $output;
    close $OUTPUT;
    open FILE, "$g_opt_path\\output.txt" or die "Cannot open $g_opt_path\\output.txt read :$!";

    while (<FILE>) {
        chomp;
        if (m/Failed   (\w+)/g ){
        print ("$1: Test module has failed\n");
        $failedMods = $failedMods."$1: Test module has failed\n";
        }
        if (m/Total tests: (\d+). Passed: (\d+). Failed: (\d+). Skipped: (\d+)./){
            print "\n \n";
            print "Total Tests:   $1 \n";
            print "Passed Tests:  $2 \n";
            print "Failed Tests:  $3 \n";
            print "Skipped Tests: $4 \n";
            if ($3>0){
                my $email = Email::Simple->create(
                    header => [
                    From    => 'vitalsignsissues@gmail.com',
                    To      => 'development@savision.com',
                    Subject => 'Cloud Advisor Test Fail_'.$g_opt_verNum.'_'.$datetime ,
                    ],
                    body => "Cloud Advisor failed the following tests:\n$failedMods\nFull Log of tests stored in $g_opt_path\\output.txt",
                );
                eval { $g_mailer->send($email) };
                die "Error sending email: $@" if $@;
            }
            else{
                local $CWD = $g_opt_path;
                my $message = "Cloud Advisor was built and tested successfully \n Please view completed build at ".PAST_BUILDS."Cloud Advisor ".$g_opt_verNum."_".$datetime;
                my $email = Email::Simple->create(
                    header => [
                        From    => 'vitalsignsissues@gmail.com',
                        To      => 'development@savision.com',
                        Subject => 'Cloud Advisor Build Completed_'.$g_opt_verNum.'_'.$datetime ,
                    ],
                    body => $message,
                );

                eval { $g_mailer->send($email) };
                die "Error sending email: $@" if $@;

                unless($g_opt_test){
                    foreach (@{$workingProd->TFSCheckIn}){
                        system('"'.$g_programs.TFEXE_PATH.'"'." checkin ".$_." /login:".LOG_CREDS." /comment:".'"'."Updated Version Number to ".'"'.$g_opt_verNum." /noprompt");
                    }
                }
            }
        }
    }
    close FILE;
}

#
#   PURPOSE:    Displays the errors that occurred while building 
#  
#   INPUT:      
#   
#   RETURNS:    OUTPUTS to command line: Build error from devenv
#
#   NOTES:      
#
sub DisplayBuildError
{
    my $dir = dir("$g_opt_path"); # /tmp

    my $logFile = $dir->file("log.txt");  #SWD: Look into variable names

    # Read in the entire contents of a file
    my $content = $logFile->slurp();

    # openr() returns an IO::File object to read from
    my $file_handle = $logFile->openr();

    # Read in line at a time
    while( my $line = $file_handle->getline() ) {
        print $line;
    } 
    my $email = Email::Simple->create(
    header => [
        From    => 'vitalsignsissues@gmail.com',
        To      => 'development@savision.com',
        Subject => 'Cloud Advisor Build Fail_'.$g_opt_verNum.'_'.$datetime ,
    ],
        body => "Cloud Advisor failed to build a project\nPlease view build log at $g_opt_path\\log.txt",
    );

    eval { $g_mailer->send($email) };
    die "Error sending email: $@" if $@;
}

#
#   PURPOSE:    Deletes temporary workspaces and deletes specified folder
#  
#   INPUT:      $g_opt_path               The user specified temporary workspace directory
#   
#   RETURNS:    None
#
#   NOTES:
#
sub DeleteWorkspace
{
    system('"'.$g_programs.TFEXE_PATH.'"'." workspace /delete ".TEMP_WORKSPACE." /login:".LOG_CREDS." -noprompt");
}

#
#   PURPOSE:    Prints script "help" information 
#  
#   INPUT:      None
#   
#   RETURNS:    None
#
#   NOTES:
#
sub Usage
{
    my $help = <<HELP_END;
Usage: $0 [--help] [--path] 

Options:
  --help,--h                        : display this information.
  --path<string>                    : specifies the local working directory TFS will check the 
                                      files into for automated build
  --version<int.int.int.int>        : updates all relevent version files to number in form of
                                      <MajorRelease>.<MajorRelease>.<Revision>.<Build>
                                      if not specified, will update build number by one
  --force                           : allows specified work folder to be deleted even if it already exists
  --label<string>                   : labels most recent TFS revision with <string>
  --past<string>                    : gets and builds solution version with label <string>
  --test                            : If used will not update tfs
  --nobuild                         : Does not build the solution
  --noversion                       : Does not open any files for version number edit
  --nodelete                        : Does not delete the workspace
  --storage<int>                    : Specifies the number of past builds to be stored
                                      if less than number of currently held builds, it will delete the excess
  --codedoc<string>                 : Specifies location for code documentation. Will create non-existing folder
               

This script is meant to help automate the building of Savision's Cloud 
Advisor System Center Virtual Machine Manager add in. It fetches the 
most recently checked in version from TFS, builds, tests and prepares the package
for manual testing/release.

Manages past builds based on dates and deletes the oldest build(s). Deletes more than
one if --storage option is specified lower than the current number of help past builds.
Past builds are stored in c:\\past builds\\

WARNING: Do not use a path that already exists for another purpose as it
will be deleted in full to ensure a successful build for testing. 
--Path <directory> will accept a directory if it doesn't exist. The script
will create, build with and delete the directory afterwards, placing the 
build file in C:\\Build\\Builds\\Components if successful. 

HELP_END

    print $help;
}

#
#   PURPOSE:    Manages Old Builds in C:\Past Builds. Deletes excess builds based on user 
#               input or the oldest build if none found. Grabs the latest build and places in  
#               appropriate folder.
#  
#   INPUT:      $g_opt_storage 
#   
#   RETURNS:    None
#
#   NOTES:      
#
sub OldBuilds
{
    my @toDelete;
    my $dh;
    my $counter=$g_opt_storage-1;
    my $name;
    while ($counter < @g_dates){
        opendir $dh, PAST_BUILDS or die "$0: opendir: $!";
        while (defined($name = readdir $dh)){
            if ($name =~ m/$g_dates[$counter]/){
                print "\n";
                push (@toDelete, $name);
                print $name;
                print "\n";
                $counter++;
                last;
                
            }
            else{
                next;
            }
        }
        closedir $dh;
    }
    unless ($g_opt_nobuild){
        foreach (@toDelete){
            rmtree(PAST_BUILDS.$_);
        }
        mkdir (PAST_BUILDS."Cloud Advisor ".$g_opt_verNum."_".$datetime);
        copy($g_opt_path.$workingProd->PostBuildFile,PAST_BUILDS."Cloud Advisor ".$g_opt_verNum."_".$datetime);
        CreateDocumentation();
    }
    CodeDocumentation();
}

#
#   PURPOSE:    Copies code documentation to specified folder and creates the directory if needed
#               if no specified folder or if creation of folder fails, the current Past Build directory
#               is used instead
#  
#   INPUT:      $g_opt_codoc
#   
#   RETURNS:    None
#
#   NOTES:      
#
sub CodeDocumentation
{

    if ($g_opt_codoc){
        print $g_opt_codoc."\n";
        unless (-e $g_opt_codoc){
            system("mkdir $g_opt_codoc");
            unless (-e $g_opt_codoc){
                $g_opt_codoc = PAST_BUILDS."Cloud Advisor ".$g_opt_verNum."_".$datetime;
            }
        }
    }
    else{
    $g_opt_codoc = PAST_BUILDS."Cloud Advisor ".$g_opt_verNum."_".$datetime;
    }
    copy($g_opt_path.$workingProd->HelpFile,$g_opt_codoc);
    
}

#
#   PURPOSE:    Creates Release Documentation upon successful build and places in correct folder
#               Uses OLE for Microsoft Word
#  
#   INPUT:      $g_opt_path, $g_opt_verNum
#   
#   RETURNS:    Outputs current version information and preview of new version number
#
#   NOTES: 
#
sub CreateDocumentation
{
    foreach (@{$workingProd->UserNotes}){
        if (m/(\w+).docx/){
            push(@documents,$1);
            print $1."\n";
        }
    }
   $Win32::OLE::Warn = 2; # Throw Errors, I'll catch them
    my $Word = Win32::OLE->GetActiveObject('Word.Application')
    || Win32::OLE->new('Word.Application', 'Quit');
    
    $Word->{'Visible'} = 0;
    for (my $county = 0; $county<@documents; $county++){            
        $Word->Documents->Open($g_opt_path.${$workingProd->UserNotes}[$county]);
        $Word->ActiveDocument->ExportAsFixedFormat({ OutputFileName => PAST_BUILDS."Cloud Advisor ".$g_opt_verNum."_".$datetime."\\".$documents[$county].".pdf",
        ExportFormat=>wdExportFormatPDF});   
        $Word->ActiveDocument->Close();
    }
    $Word->Quit;

}

sub Uncheckout
{
    foreach(@{$workingProd->TFSCheckOut}){
        if ($g_opt_test){
            unless ( $_ =~m/MASTER_VERSION.txt/){
            system('"'.$g_programs.TFEXE_PATH.'"'." undo ".$_." /noprompt /workspace:".TEMP_WORKSPACE." /login:".LOG_CREDS);

            }
        }
        else{
            unless ( $_ =~m/MASTER_VERSION.txt/){
                system('"'.$g_programs.TFEXE_PATH.'"'." undo ".$_." /noprompt /workspace:".TEMP_WORKSPACE." /login:".LOG_CREDS);

            }
        }
    }
}



#
#   PURPOSE:    Updates all files containing the Version number to match version number pulled from Version.Txt
#               under each project's properties page. Can accept user specified $g_opt_verNum values to jump versions
#  
#   INPUT:      $g_opt_path, $g_opt_verNum
#   
#   RETURNS:    Outputs current version information and preview of new version number
#
#   NOTES: 
#
sub UpdateVersion   
{
    my $tempText;
    my $File;
    my $filePath;
    local $CWD = $g_opt_path;

        unless ($g_spec_ver){
            $g_opt_verNum = "$g_majorRelease.$g_minorRelease.$g_versionNumber.$g_buildNumber";
            print "Major Release:    $g_majorRelease \n";
            print "Minor Release:    $g_minorRelease \n";
            print "Revision Number:  $g_versionNumber \n";
            print "Build Number:     $g_buildNumber \n";
            open  $File,">", $g_opt_path.$workingProd->VersionFile or die "Cannot open $g_opt_path.$workingProd->VersionFile :$!";
            print $File $g_opt_verNum;
            close $File;
        }
    
    foreach (@{$workingProd->UpdateFiles}){
        $tempText = undef;
        $filePath= $g_opt_path.$_ ;
        open $File, $filePath or die "Cannot open $filePath read :$!";
        while (<$File>){
            s/AssemblyVersion("(\d+).(\d+).(\d+).(\d+)")/AssemblyVersion("$g_opt_verNum")/g;
            s/Version="(\d+).(\d+).(\d+).(\d+)"/Version="$g_opt_verNum"/g;
            s/AssemblyFileVersion("(\d+).(\d+).(\d+).(\d+)")/AssemblyFileVersion("$g_opt_verNum")/g;
            $tempText =$tempText.$_;
        }
        close $File;
        open $File, "+<" , $filePath or die "Cannot open $filePath read :$!";
        print $File $tempText;
        close $File;
    }
}

#
#   PURPOSE:    Labels current TFS solution with user specified label
#  
#   INPUT:      $g_opt_path, $g_opt_Label
#   
#   RETURNS:    N/A
#
#   NOTES: 
#
#sub Label
#{
#    local $CWD = $g_opt_path;
#    system('"'.$g_programs.TFEXE_PATH.'"'." get ".$workingProd->TFSGetPath." /login:".LOG_CREDS." /recursive /force ");
#    system('"'.$g_programs.TFEXE_PATH.'"'." label ".$g_opt_label." ".$workingProd->TFSGetPath." /login:".LOG_CREDS." /recursive");
#}

#
#   PURPOSE:    Grabs version of solution from TFS with specified label
#  
#   INPUT:      $g_opt_path, $g_opt_Label
#   
#   RETURNS:    N/A
#
#   NOTES: 
#
sub GetLabel
{
    local $CWD = $g_opt_path;
    system('"'.$g_programs.TFEXE_PATH.'"'." get ".'/version:L"'.$g_opt_past.'" '.$workingProd->TFSGetPath." /login:".LOG_CREDS." /recursive /force ");
}