import logging
import subprocess
# import shutil
# import tempfile

class Job(object):
    """
    The Job class represents a job running through the cluster queue system.
    """
    
    def __init__(self, manager):
        self.manager = manager
        self.replica = None
        self.node = None
    
    def submit(self):
        """ Submit the job to a node by using qsub """
        print 'Submitting job...'
    
    def status(self):
        """ Get the status of a job. Will determine Node and Replica information if possible """
        if self.replica:
            return self.replica.status
        else:
            return 'No replica'
        

class Node(object):
    """
    The Node class represents a cluster node.
    Using this class you can find out what's happening on a given node.
    """
    pass

# Subclass Replica to handle different simulation packages and systems?
class Replica(object):
    """
    The Replica class represents an individual replica that is run on a node.
    It contains all the information necessary for a new job to run a replica on a node.
    
    Original struct replica_struct in DR:
        char status;
    	float w;
    	float w_nominal;
    	float w_start;
    	float w2_nominal;
    	float w_sorted;
    	float force;
    	unsigned int sequence_number;
    	unsigned int sample_count;
    	unsigned int sampling_runs;
    	unsigned int sampling_steps;
    	double cancellation_accumulator[2];
    	unsigned short cancellation_count;
    	float cancellation_energy;
    	unsigned int last_activity_time;
    	unsigned int start_time_on_current_node;
    	struct buffer_struct restart;
    	struct atom_struct *atom;
    	unsigned int *presence;
    	char vREfile[500];
    	int nodeSlot;
	
    """
    
    ERROR = 'error'
    RUNNING = 'running'
    READY = 'ready'
    FINISHED = 'finished'
    
    def __init__(self, id):
        self.id = id
        self.status = self.READY
        
    def __repr__(self):
        return '<Replica %d:%s>' % (self.id, self.status)
        
    def run(self):
        """ The actual code that runs the replica on the client node """
        print 'Running replica %d' % self.id
        return_code = subprocess.call(['sleep', '10'], 0, None, None)
        print 'Replica %d finished' % self.id
        return return_code
        
if __name__ == '__main__':
    print "This is a library!"