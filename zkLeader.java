package es.upm.dit.cnvr.lab1;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class zkLeader implements Watcher {
	
	private static final int SESSION_TIMEOUT = 5000;
	private static String rootMembers = "/members";
	private static String aMember = "/member-";
	private String myId;
	private String leaderId; // Para almacenar el líder actual
	private Lock lock = new ReentrantLock();
	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"localhost:2181", "localhost:2182", "localhost:2183"};
	
	private ZooKeeper zk;

	public zkLeader () {

		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				try {
					// Wait for creating the session. Use the object lock
					lock.lock();
					System.out.println("Cerrado el cerrojo");
					//zk.exists("/",false);
				} catch (Exception e) {
					System.err.println(e);
				}
			}
		} catch (Exception e) {
			System.out.println("Error");
		}
		
		// Add the process to the members in zookeeper
		
		if (zk != null) {
			
			// Create a folder for members and include this process/server
			try {
				// Create a folder, if it is not created
				String response = new String();
				Stat s = zk.exists(rootMembers, watcherMember); //this);
				
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(rootMembers, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);

				}
				// Create a znode for registering as member and get my id
				myId = zk.create(rootMembers + aMember, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
				
				myId = myId.replace(rootMembers + "/", "");
				
				List<String> list = zk.getChildren(rootMembers, watcherMember, s); //this, s);
				System.out.println("Created znode nember id:"+ myId );
				electLeader(list);
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
	}
	
	private void electLeader(List<String> members) {
        // Encuentra el miembro con el menor ID
        leaderId = members.stream().sorted().findFirst().orElse(null);

        if (leaderId != null && leaderId.equals(myId)) {
            System.out.println("Soy el líder actual. Mi ID: " + leaderId);
        } else {
            System.out.println("El líder actual es: " + leaderId);
        }
    }
	
	// Notified when the session is created
	private Watcher cWatcher = new Watcher() {
		public void process (WatchedEvent e) {
			System.out.println("Created session");
			System.out.println(e.toString());
			try {
				lock.unlock(); //notify();
			} catch (Exception exception) {
				System.out.println("Exception: Notified when the sesion is created");
			}
		}
	};

	// Notified when the number of children in /member is updated
	private Watcher  watcherMember = new Watcher() {
		public void process(WatchedEvent event) {
			System.out.println("------------------Watcher Member------------------\n");		
			try {
				System.out.println("        Update!!");
				List<String> list = zk.getChildren(rootMembers,  watcherMember); //this);
				electLeader(list);
			} catch (Exception e) {
				System.out.println("Exception: wacherMember");
			}
		}
	};
	
	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
			List<String> list = zk.getChildren(rootMembers, watcherMember); //this);
			electLeader(list);
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}
	
	private void printListMembers (List<String> list) {
		System.out.println("Remaining # members:" + list.size());
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}
	
	public static void main(String[] args) {
		zkLeader zk = new zkLeader();

		try {
			Thread.sleep(300000); 			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
}