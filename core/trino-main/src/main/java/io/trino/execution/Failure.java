/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("ExceptionClassNameDoesntEndWithException")
public class Failure
        extends RuntimeException
{
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    private final ExecutionFailureInfo failureInfo;

    Failure(ExecutionFailureInfo failureInfo)
    {
        super(failureInfo.getMessage(), failureInfo.getCause() == null ? null : new Failure(failureInfo.getCause()));
        this.failureInfo = failureInfo;

        for (ExecutionFailureInfo suppressed : failureInfo.getSuppressed()) {
            addSuppressed(new Failure(suppressed));
        }
        setStackTrace(failureInfo.getStack().stream()
                .map(Failure::toStackTraceElement)
                .toArray(StackTraceElement[]::new));
    }

    public ExecutionFailureInfo getFailureInfo()
    {
        return failureInfo;
    }

    @Override
    public String toString()
    {
        String type = getFailureInfo().getType();
        String message = getMessage();
        if (message != null) {
            return type + ": " + message;
        }
        return type;
    }

    private static StackTraceElement toStackTraceElement(String stack)
    {
        Matcher matcher = STACK_TRACE_PATTERN.matcher(stack);
        if (matcher.matches()) {
            String declaringClass = matcher.group(1);
            String methodName = matcher.group(2);
            String fileName = matcher.group(3);
            int number = -1;
            if (fileName.equals("Native Method")) {
                fileName = null;
                number = -2;
            }
            else if (matcher.group(4) != null) {
                number = Integer.parseInt(matcher.group(4));
            }
            return new StackTraceElement(declaringClass, methodName, fileName, number);
        }
        return new StackTraceElement("Unknown", stack, null, -1);
    }
}
