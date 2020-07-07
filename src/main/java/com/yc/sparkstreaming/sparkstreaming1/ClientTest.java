package com.yc.sparkstreaming.sparkstreaming1;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class ClientTest {
	public static void main(String[] args) throws IOException {
		ServerSocket ss=new ServerSocket(9999);
		boolean flag=true;
		while(flag){
			Socket s=ss.accept();  //只要有客户端连接  生成新线程处理
			new Thread(new Task(s)).start();
		}
	}
}

class Task implements Runnable{
	private Socket s;
	public Task(Socket s){
		this.s=s;
	}
	
	@Override
	public void run() {
		Random r=new Random();
		boolean flag=true;
		try {
			OutputStream oos=s.getOutputStream();
			PrintWriter out=new PrintWriter(oos,true);
			while(flag){
				Thread.sleep(r.nextInt(3000));
				String s="char:"+(char)r.nextInt(128); //ASCII码
				out.println(s);
				out.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
}
