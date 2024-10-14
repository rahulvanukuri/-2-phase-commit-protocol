import threading
import time
import random

class VoterNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.status = "UNKNOWN"
        self.transaction_data = None
    def receive_prepare(self, transaction_data,T_C):
        
        if self.status == "UNKNOWN" and T_C:
            self.transaction_data = transaction_data
            self.status = "PREPARED"
            return "YES"
        else:
            return "NO"
    def receive_commit(self):
        
        if self.status == "PREPARED" or self.status== 'COMMITTED':
            self.status = "COMMITTED"
            return "COMMIT"
        else:
            return "ACK"
    def receive_abort(self):
        
        if self.status == "PREPARED" or self.status == "UNKNOWN":
            self.status = "ABORTED"
            return "ABORT"
        else:
            return "ACK"


class Coordinator:
    def __init__(self, voter_nodes):
        self.voter_nodes = voter_nodes

    def send_prepare_message(self, transaction_data,T_C):
        decision = []
        if(T_C):
            for node in self.voter_nodes:
                response = node.receive_prepare(transaction_data,T_C)
                decision.append(response)
            return decision
        else:
            return self.send_abort_message()

    def send_commit_message(self):
        decision = []
        for node in self.voter_nodes:
            response = node.receive_commit()
            decision.append(response)
        return decision

    def send_abort_message(self):
        decision = []
        for node in self.voter_nodes:
            response = node.receive_abort()
            decision.append(response)
        return decision


def main():
    
    voter1 = VoterNode(1)
    voter2 = VoterNode(2)
    voter3 = VoterNode(3)

    voter_nodes = [voter1, voter2, voter3]

    tc = Coordinator(voter_nodes)
    while True:
        print("a. TC failure before sending prepare message" )
        print("b. Node failure before sending yes response to prepare message")
        print("c. TC failure after sending yes response to prepare message")
        print("d. Node failure after sending yes response to prepare message")
        print("e. Exit")
        opt = input("Enter your Option")
        # Case 1: TC failure before sending prepare message
        if opt == 'a':
            T_C=False
            decision = tc.send_prepare_message("Transaction Data",T_C)
            print(decision)
            time.sleep(5)
            T_C=True
            decision = tc.send_prepare_message("Transaction Data",T_C)
            print("Responses:", decision)

        # Case 2: Node failure before sending yes response to prepare message
        elif opt == 'b':
            T_C=True
            print("Simulating Node failure before sending yes response to prepare message...")
            decision = tc.send_prepare_message("Transaction Data",T_C)
            index = random.randint(0, len(voter_nodes)-1)
            failed_node = voter_nodes[index]
            failed_node.status = "UNKNOWN"
            print(failed_node.node_id)
            decision[index]='NO'
            print(decision)
            time.sleep(5)
            decision = tc.send_prepare_message("Transaction Data",T_C)
            for i in range(0,len(voter_nodes)):
                if(decision[i]=='YES'):
                    print(i)
                    decision[i]='Abort'
                else:
                    decision[i]='Commit'
            print("Responses:", decision)
    
        # Case 3: TC failure after sending yes response to prepare message
        elif opt == 'c':
            text_file = open("TCfailure.txt", "w")
            T_C=True
            print("Simulating TC failure after sending commit message...")
            text_file.write("Before TC failure:\n")
            decision = tc.send_prepare_message("Transaction Data",T_C)
            for i in decision:
                text_file.write('%s ' % i)
            text_file.write('\n')
            decision = tc.send_commit_message()

            text_file.write("TC Failed After 1st commit at a node\n")
            index = random.randint(0, len(voter_nodes)-1)
            print(index)
            for i in range(index,len(voter_nodes)):
                failed_node = voter_nodes[i]
                failed_node.status = "ACK"
                decision[i]="ACK"
            for i in decision:
                text_file.write('%s ' % i)
            text_file.write('\n')
        
            print(decision)
            text_file.write('nodes after %s failes after 1st commit and gets ACK and becomes prepared when TC comes back\n' % index)
            time.sleep(5)
            tc.voter_nodes = []
            for i in voter_nodes:
                if i.status== 'ACK':
                    i.status='PREPARED'
                    tc.voter_nodes.append(i)
                text_file.write('%s ' % i.status)   
            text_file.write('\n')
            decision = tc.send_commit_message()
            text_file.write('TC comes back and commit the remaining nodes\n')
            for i in decision:
                text_file.write('%s ' % i)
            text_file.write('\n')
            print("Responses:", decision)
            text_file.close()

        # Case 4: Node failure after sending yes response to prepare message
        elif opt == 'd':
            T_C=True
            text_file = open("NodeFailure.txt", "w")
            text_file.write("Before TC failure:\n")
            decision = tc.send_prepare_message("Transaction Data",T_C)
            for i in decision:
                text_file.write('%s ' % i)
            text_file.write('\n')
            decision = tc.send_commit_message()
            fail_node = None
            index = random.randint(0, len(voter_nodes)-1)
            fail_node = voter_nodes[index]
            text_file.write('nodes %s failes after 1st commit and gets ACK and becomes prepared when TC comes back\n' % index)
            fail_node.status = "ACK"
            decision[index]='ACK'
            print(decision)
            time.sleep(5)
            tc.voter_nodes = []
            for i in voter_nodes:
                if i.status== 'ACK':
                    i.status='PREPARED'
                    tc.voter_nodes.append(i)
                text_file.write('%s ' % i.status) 

            decision = tc.send_commit_message()
            print("Responses:", decision)
            text_file.close()
        elif opt=='e':
            exit()
        else:
            print("Please enter only Valid options")

    # Reset the status of all nodes
    for node in voter_nodes:
        node.status = "UNKNOWN"

if __name__ == "__main__":
    main()