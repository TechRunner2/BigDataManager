#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 15:19:46 2018

@author: techrunner
This is a small piece of software writen to help manage the big data cluster
This program is for use with the UMHB big data cluster
It is not perfect but it will get some things done quickly
"""
import os
import json

class Manager:
    def __init__(self):
        try:
            file = open('bigdata.json','r')
            self.configs = json.load(file)
        except:
            file = open('.bigdata.cfg','w+')
            config = {
                'host':'10.14.20.75',
                'ports':'2222-2225',
                'admin':'root',
                'admin_password':'adminuser',
                'username':'biguser',
                'password':'biguser',
                'amount':15,
                'groups':'BigData'
                }
            json.dump(config, file)
            self.configs = config

 
    def get_ports(self):
        """Gets ports from configs and outputs a list"""
        plist = []
        if '-' in self.configs['ports']:
            ports = self.configs['ports'].split('-')
            for port in range(int(ports[0]),(int(ports[1])+1)):
                plist.append(port)
        else:
            ports = self.configs['ports'].split(',')
            for x in range(len(ports)):
                plist.append(int(ports[x]))
        return plist
    
    def get_groups(self):
        """Get groups from config list and output a string with all groups"""
        group_string = ''
        if ',' in self.configs['groups']:
            groups = self.configs['groups'].split(',')
            for x in range(len(groups)):
                if x == 0:
                    group_string = groups[x]+','
                elif x == len(groups):
                    group_string += groups[x]
                else:
                    group_string += groups[x]+','
            return group_string
        else:
            return self.configs['groups']
        
    def get_users(self):
        """Get user list from configs dictionary and output a list"""
        if ',' in self.configs['username']:
            users = self.configs['username'].split(',')
        elif self.configs['amount'] > 0 and not ',' in self.configs['username']:
            users = []
            for x in range(int(self.configs['amount'])+1):
                users.append(self.configs['username']+str(x).zfill(2))
        return users
                
    def create_users(self):
        """Create user accounts for students"""
        users = self.get_users()
        groups = self.get_groups()
        for user in users:
            self.send(f'useradd -m -G {groups} {user}')
        
    def create_groups(self):
        """Create groups on virtual hosts"""
        groups = self.get_groups().split(',')
        for x in groups:
            self.self.send(f'groupadd {x}')
            
    def delete_groups(self):
        """Detete all groups that are in configs"""
        groups = self.get_groups().split(',')
        for x in groups:
            self.send(f'groupdel {x}')
            
    def create_hadoop(self):
        """Create Hadoop Space for users on VM's"""
        users = self.get_users()
        for x in users:
            self.send(f'mkdir -p /user/{x}')
            
    def delete_hadoop(self):
        """Delete Hadoop Space for users on VM's"""
        users = self.get_users()
        for x in users:
            self.send(f'rm -r /usr/{x}')
    
    def add_passwords(self):
        passwords = self.get_users()
        for x in passwords:
            self.send(f'(echo {x}; echo {x})| passwd {x}')
        
        
    def send(self, command):
        """Send command to servers using ssh"""
        if command.startswith('ls'):
            command = f'pwd && ls{command[2:]}'
        ports = self.get_ports()
        computer = 1
        for x in ports:
            print(f'{computer:*^10}')
            print(os.popen(f'ssh -p {x} {self.configs["admin"]}@{self.configs["host"]} \'{command}\'').read())
            computer += 1
            
    def main(self):
        """Run program"""
        while True:
            print('Would you like to do?\n1. Edit Configs\n2. Make Groups\n3. Make Users\n4. Make Hadoop Spaces\n5. Send command\n-1: Exit', end='')
            user_input = input(': ')
            commands = {'1':self.create_hadoop,
                        '2':self.create_groups,
                        '3':self.create_users,
                        '4':self.create_hadoop,
                        '5':self.send
                        }
            if user_input == '5':
                command = input('$: ')
                commands['5'](command)
            elif user_input == '-1':
                break
            elif user_input in '123456789010-1':
                commands[user_input]()
            else:
                print(f'\n\n{"Invalid Command":*^40}\n\n')

if __name__ == '__main__':
    BD = Manager()
    BD.main()