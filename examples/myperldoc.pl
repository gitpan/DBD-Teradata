#
#	Since perldoc, pod2text, and various other formatters
#	can't redirect output properly in Windows, we've got to roll
#	our own...sheeeesh!
#
use Pod::Text;

pod2text($ARGV[0]);
