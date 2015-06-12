
/*
 * Copyright 2015 Rackspace
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.rackspacecloud.blueflood.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GenericClassLoader {
    private static final Logger log = LoggerFactory.getLogger(GenericClassLoader.class);
    private static Object genericInstance;
    public static Object getGenericInstance(Class c, String module){


        log.info("Loading the module " + module);
        System.out.println("Loading the module " + module);

        System.out.println("Class Name --> "+c.getName());
        if (genericInstance != null)
            return genericInstance;

        try {
            ClassLoader loader = c.getClassLoader();
            Class genericClass = loader.loadClass(module);
            genericInstance = genericClass.newInstance();
            log.info("Registering the module " + module);
        }
        catch (InstantiationException e) {
            log.error("Unable to create instance of "+ c.getName()+" class for: " + module, e);
            System.out.println("Unable to create instance of "+ c.getName()+" class for: " + module);
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            log.error("Error starting module: " + module, e);
            System.out.println("Error starting module: " + module);
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            log.error("Unable to locate module: " + module, e);
            System.out.println("Unable to locate module: " + module);
            e.printStackTrace();
        } catch (RuntimeException e) {
            log.error("Error starting module: " + module, e);
            System.out.println("Error starting module: " + module);
            e.printStackTrace();
        } catch (Throwable e) {
            log.error("Error starting module: " + module, e);
            System.out.println("Error starting module: " + module);
            e.printStackTrace();
        }


        return genericInstance;
    }
}
