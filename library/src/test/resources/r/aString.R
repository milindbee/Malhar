#!/usr/bin/Rscript

# This script takes two strings and returns a concatenated string from both of 
# the individual strings. The concatenation is done using the characters "--".
# iThe argument "collapse" specifies the character(s) to be used between the 
# elements of the vector to be collapsed.
# Used to test the 'STR' data type(s) being passed from and to the 
# Rscript operator for Malhar.
# str1 and str2 are passed as arguments by the client and retVal is the 
# result returned by the script.

# The following lines to be uncommented to test the script
#
# args<-commandArgs(TRUE)
# str1<-args[1]
# str2<-args[2]
# seperator<-args[3]

retVal<-paste(str1, str2, sep=seperator)
return(retVal)
