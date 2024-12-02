package es.upm.dit.cnvr.lab1;

import java.nio.ByteBuffer;
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

public class zkCounter implements Watcher {
	
	private static final int SESSION_TIMEOUT = 5000;
	private static String counters = "/counters";
	private static String cola = "/cola-";
	private static String dataPath = "/data";
	private String myId;
	private String counterId; // Para almacenar el líder actual
	private Lock lock = new ReentrantLock();
	private static Integer maxcounter = 10000;
	private Integer currentvalue;
	
	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"localhost:2181", "localhost:2182", "localhost:2183"};
	
	private ZooKeeper zk;

	public zkCounter () {
		byte[] data;
		// Select a random zookeeper server
		Random rand = new Random();
		int i = rand.nextInt(hosts.length);

		// Create a session and wait until it is created.
		// When is created, the watcher is notified
		try {
			if (zk == null) {
				zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
				
			}
		} catch (Exception e) {
			System.out.println("Error");
		}
		
		// Add the process to the counters in zookeeper
		
		if (zk != null) {
			
			// Create a folder for counters and include this process/server
			try {
				// Create a folder, if it is not created
				
			
				String response = new String();
				Stat s = zk.exists(counters, watcherMember); //this);
				
				if (s == null) {
					// Created the znode, if it is not created.
					response = zk.create(counters, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(response);
				}			
				// Create a znode for registering as counters and get my id
				myId = zk.create(counters + cola, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);		
				myId = myId.replace(counters + "/", "");
				
				s = zk.exists(dataPath, false);
				if (s == null) {
					data = ByteBuffer.allocate(4).putInt(0).array();
					zk.create(dataPath, data,
							Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				}
						
				List<String> list = zk.getChildren(counters, watcherMember, s); //this, s);
				System.out.println("Created znode nember id:"+ myId );
				electCounter(list);
			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}
		}
	}
	
	private void electCounter(List<String> counters) {
        // Encuentra el miembro con el menor ID
        counterId = counters.stream().sorted().findFirst().orElse(null);

        if (counterId != null && counterId.equals(myId)) {
            System.out.println("El contador actual es . Mi ID: " + counterId);
        } else {
            System.out.println("El contador actual es " + counterId);
        }    
    }
	
	private void deleteElectedCounter() {
	    try {
	        // Eliminar el znode del miembro con el menor ID (counterId)
	        if (counterId != null) {
	            String pathToDelete = counters + "/" + counterId;
	            zk.delete(pathToDelete, -1); // -1 ignora la versión del nodo
	            System.out.println("Nodo eliminado: " + pathToDelete);
	        } else {
	            System.out.println("No se pudo encontrar el nodo a eliminar (counterId es nulo).");
	        }
	    } catch (KeeperException e) {
	        System.err.println("Error al eliminar el nodo del contador: " + e.getMessage());
	    } catch (InterruptedException e) {
	        System.err.println("Interrupción mientras se eliminaba el nodo del contador.");
	    }
	}
	
	private void incrementCounter() throws InterruptedException {

			Stat s;
			byte[] data;
			//List<String> list = zk.getChildren(counters,  watcherMember); //this);
			
	        while (counterId == null || !counterId.equals(myId)) {
	        	
        		System.out.println("El contador actual es " + counterId +" Yo soy el contador" + myId);
				Thread.sleep(10);
	        }
				try {
					s = new Stat();
					Random random = new Random();
					int increment = random.nextInt(5) + 1;
					System.out.println(increment);
					data = zk.getData(dataPath, false, s);
					currentvalue = ByteBuffer.wrap(data).getInt();
					System.out.println(currentvalue);
					currentvalue += 1;
					data = ByteBuffer.allocate(4).putInt(currentvalue).array();
					if (currentvalue < maxcounter) {
						try {
							zk.setData(dataPath, data, -1);
							System.out.println("El valor del contador es: " + currentvalue);
						}catch (Exception e) {
					        System.err.println("Error en incrementar value");
					    }
					} else {
						System.out.println("Contador llego a 100");}
			}catch (Exception e) {
		        System.err.println("Error en incrementar value");
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
				List<String> list = zk.getChildren(counters,  watcherMember); //this);
				electCounter(list);
			} catch (Exception e) {
				System.out.println("Exception: wacherMember");
			}
		}
	};
	
	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
			List<String> list = zk.getChildren(counters, watcherMember); //this);
			electCounter(list);
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}
	
	public static void main(String[] args) throws KeeperException, InterruptedException {
		int salida = 0;
		zkCounter zk = new zkCounter();
		for(int i = 0; i< 100; i++) {
			zk.incrementCounter();
			
			try {
			// En la evaluación: Thread.sleep(1);
			
			Thread.sleep(1);
			}catch (Exception e) {
			System.out.println("Error" + e);
			}
			}
		zk.deleteElectedCounter();
	}
}