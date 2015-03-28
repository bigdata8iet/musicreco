package com.iet.bigdata.musicreco.reco;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Scanner;

public class logindemo {

	public static void main(String[] args) throws Exception {
		FileInputStream fs=new FileInputStream("/mnt/data/workspace/music/test/logintest");
		InputStreamReader isr=new InputStreamReader(fs);
		BufferedReader br=new BufferedReader(isr);
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter Username");
		String name = sc.nextLine();
		System.out.println("Enter Password");
		String psd = sc.nextLine();
		
		
		String line=null, str[] = new String[3];
		int uid=0,flag=0;
		while((line=br.readLine())!=null){
			str=line.split(",", 3);
			
			if(name.equalsIgnoreCase(str[1])){
				flag = 1;
				if(psd.equalsIgnoreCase(str[2])){
					System.out.println("Login Successful....");
					uid = Integer.parseInt(str[0]);
					System.out.println("UserId is" +uid);
					break;
				}
				else {
					System.out.println("Password incorrect");
					break;
				}	
			}
		}
		 if(flag==0)
			System.out.println("Username doesn't exist");
	}

}