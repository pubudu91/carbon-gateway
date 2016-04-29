/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * <p>
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.gateway.mediators.samplemediator;

import net.sf.saxon.Configuration;
import net.sf.saxon.lib.NamespaceConstant;
import net.sf.saxon.om.DocumentInfo;
import net.sf.saxon.xpath.XPathFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.gateway.core.config.ParameterHolder;
import org.wso2.carbon.gateway.core.flow.AbstractMediator;
import org.wso2.carbon.gateway.core.flow.contentaware.ByteBufferBackedInputStream;
import org.wso2.carbon.messaging.CarbonCallback;
import org.wso2.carbon.messaging.CarbonMessage;
import org.xml.sax.InputSource;

import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.transform.sax.SAXSource;
import javax.xml.xpath.*;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Sample Custom Mediator
 */
public class SampleCustomMediator extends AbstractMediator implements XPathVariableResolver, NamespaceContext {

    private static final Logger log = LoggerFactory.getLogger(SampleCustomMediator.class);
    private String logMessage = "Message received at Custom Sample Mediator";

    public SampleCustomMediator() {
    }

    public void setParameters(ParameterHolder parameterHolder) {
        logMessage = parameterHolder.getParameter("parameters").getValue();
    }

    @Override
    public String getName() {
        return "SampleCustomMediator";
    }

    @Override
    public boolean receive(CarbonMessage carbonMessage, CarbonCallback carbonCallback) throws Exception {
        BlockingQueue<ByteBuffer> content = new LinkedBlockingQueue<>(carbonMessage.getFullMessageBody());
        InputStream stream = new ByteBufferBackedInputStream(content);
        go(stream, "//LINE");
        return next(carbonMessage, carbonCallback);
    }

    public void go(InputStream stream, String query) throws Exception {

        // Following is specific to Saxon: should be in a properties file
        System.setProperty("javax.xml.xpath.XPathFactory:" + NamespaceConstant.OBJECT_MODEL_SAXON,
                "net.sf.saxon.xpath.XPathFactoryImpl");

        XPathFactory xpf = XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON);
        XPath xpe = xpf.newXPath();
        System.err.println("Loaded XPath Provider " + xpe.getClass().getName());

        long startTime = System.currentTimeMillis();
        // Build the source document. This is outside the scope of the XPath API, and
        // is therefore Saxon-specific.
        InputSource is = new InputSource(stream);
        SAXSource ss = new SAXSource(is);
        Configuration config = ((XPathFactoryImpl) xpf).getConfiguration();
        DocumentInfo doc = config.buildDocument(ss);

        // Declare a variable resolver to return the value of variables used in XPath expressions
        xpe.setXPathVariableResolver(this);

        XPathExpression findLine = xpe.compile(query);

        List matchedLines = (List) findLine.evaluate(doc, XPathConstants.NODESET);

        long endTime = System.currentTimeMillis();

//        Utils.printResult(endTime - startTime, matchedLines.size() + "");
        log.info("Time elapsed: " + (endTime - startTime));
        log.info("Elements found: " + matchedLines.size());
    }

    public String getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(String logMessage) {
        this.logMessage = logMessage;
    }

    @Override
    public Object resolveVariable(QName qName) {
        return null;
    }

    public String getNamespaceURI(String prefix) {
        System.err.println("Looking up: " + prefix);
        if (prefix.equals("saxon")) {
            return "http://saxon.sf.net/";
        } else {
            return null;
        }
    }

    @Override public String getPrefix(String namespaceURI) {
        return null;
    }

    @Override public Iterator getPrefixes(String namespaceURI) {
        return null;
    }
}
