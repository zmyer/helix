package org.apache.helix.model.builder;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.model.Transition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: 2018/6/4 by zmyer
public class StateTransitionTableBuilder {
    // for convenient get path value, in which non-exist means MAX
    static int getPathVal(Map<String, Map<String, Integer>> path, String fromState, String toState) {
        if (!path.containsKey(fromState)) {
            return Integer.MAX_VALUE;
        }

        if (!(path.get(fromState).containsKey(toState))) {
            return Integer.MAX_VALUE;
        }

        return path.get(fromState).get(toState);
    }

    // TODO: 2018/7/26 by zmyer
    static void setPathVal(Map<String, Map<String, Integer>> path, String fromState, String toState, int val) {
        if (!path.containsKey(fromState)) {
            path.put(fromState, new HashMap<String, Integer>());
        }

        path.get(fromState).put(toState, val);
    }

    // TODO: 2018/7/26 by zmyer
    static void setNext(Map<String, Map<String, String>> next, String fromState, String toState,
            String nextState) {
        if (!next.containsKey(fromState)) {
            next.put(fromState, new HashMap<String, String>());
        }

        next.get(fromState).put(toState, nextState);

    }

    /**
     * auxiliary method to get next state based on next map
     * @param next
     * @param fromState
     * @param toState
     * @return nextState or null if doesn't exist a path
     */
    // TODO: 2018/7/26 by zmyer
    public static String getNext(Map<String, Map<String, String>> next, String fromState,
            String toState) {
        if (!next.containsKey(fromState)) {
            // no path
            return null;
        }

        return next.get(fromState).get(toState);
    }

    // debug
    static void printPath(List<String> states, Map<String, Map<String, String>> next) {
        for (String fromState : states) {
            for (String toState : states) {
                if (toState.equals(fromState)) {
                    // not print self-loop
                    continue;
                }

                System.out.print(fromState);
                String nextState = getNext(next, fromState, toState);
                while (nextState != null && !nextState.equals(toState)) {
                    System.out.print("->" + nextState);
                    nextState = getNext(next, nextState, toState);
                }

                if (nextState == null) {
                    // no path between fromState -> toState
                    System.out.println("->null" + toState + " (no path avaliable)");
                } else {
                    System.out.println("->" + toState);
                }
            }
        }
    }

    /**
     * Uses floyd-warshall algorithm, shortest distance for all pair of nodes
     * Allows one to lookup nextState given fromState,toState <br/>
     * map.get(fromState).get(toState) --> nextState
     * @param states
     * @param transitions
     * @return next map
     */
    // TODO: 2018/7/26 by zmyer
    public Map<String, Map<String, String>> buildTransitionTable(List<String> states,
            List<Transition> transitions) {
        // path distance value
        Map<String, Map<String, Integer>> path = new HashMap<String, Map<String, Integer>>();

        // next state
        Map<String, Map<String, String>> next = new HashMap<String, Map<String, String>>();

        // init path and next
        for (final String state : states) {
            setPathVal(path, state, state, 0);
            setNext(next, state, state, state);
        }

        for (final Transition transition : transitions) {
            final String fromState = transition.getFromState();
            final String toState = transition.getToState();
            setPathVal(path, fromState, toState, 1);
            setNext(next, fromState, toState, toState);
        }

        // iterate
        for (final String intermediateState : states) {
            for (final String fromState : states) {
                for (final String toState : states) {
                    final int pathVal1 = getPathVal(path, fromState, intermediateState);
                    final int pathVal2 = getPathVal(path, intermediateState, toState);
                    final int pathValCur = getPathVal(path, fromState, toState);

                    // should not overflow
                    if (pathVal1 < Integer.MAX_VALUE && pathVal2 < Integer.MAX_VALUE
                            && (pathVal1 + pathVal2) < pathValCur) {
                        setPathVal(path, fromState, toState, pathVal1 + pathVal2);
                        setNext(next, fromState, toState, getNext(next, fromState, intermediateState));
                    }
                }
            }
        }
        return next;
    }

    // TODO move this to test
    public static void main(String[] args) {
        List<String> states = new ArrayList<String>();
        // [MASTER, SLAVE, DROPPED, OFFLINE]
        // [SLAVE-OFFLINE, OFFLINE-SLAVE, SLAVE-MASTER, OFFLINE-DROPPED, MASTER-SLAVE]
        states.add("MASTER");
        states.add("SLAVE");
        states.add("DROPPED");
        states.add("OFFLINE");

        List<Transition> transitions = new ArrayList<Transition>();
        transitions.add(new Transition("SLAVE", "OFFLINE"));
        transitions.add(new Transition("OFFLINE", "SLAVE"));
        transitions.add(new Transition("SLAVE", "MASTER"));
        transitions.add(new Transition("OFFLINE", "DROPPED"));
        transitions.add(new Transition("MASTER", "SLAVE"));

        StateTransitionTableBuilder builder = new StateTransitionTableBuilder();
        Map<String, Map<String, String>> next = builder.buildTransitionTable(states, transitions);
        System.out.println(next);
        printPath(states, next);
    }
}
