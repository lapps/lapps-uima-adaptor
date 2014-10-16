package org.lappsgrid.uima.adaptor.xmi;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.AbstractCas;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.component.JCasMultiplier_ImplBase;
import org.apache.uima.fit.descriptor.OperationalProperties;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.lappsgrid.api.Data;
import org.lappsgrid.client.ServiceClient;
import org.xml.sax.SAXException;

import javax.xml.rpc.ServiceException;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author Di Wang.
 */
@OperationalProperties(outputsNewCases = true)
public class UimaXmiClientAdaptor extends JCasMultiplier_ImplBase {

  public static final String PARAM_ENDPOINT = "endpoint";

  public static final String PARAM_USERNAME = "user";

  public static final String PARAM_PASSWORD = "password";

  public static final String CAS_DISCRIMINATOR = "http://vocab.lappsgrid.org/ns/media/xml#uima-cas";

  public static final Charset CHARSET = StandardCharsets.UTF_8;

  private Boolean mFailOnUnknownType = true;

  private ServiceClient serviceClient;

  private JCas cas;

  private boolean serviceExecuted;

  @Override
  public void initialize(UimaContext context) throws ResourceInitializationException {
    super.initialize(context);
    String serviceEndpoint = (String) context.getConfigParameterValue(PARAM_ENDPOINT);
    String username = (String) context.getConfigParameterValue(PARAM_USERNAME);
    String password = (String) context.getConfigParameterValue(PARAM_PASSWORD);
    try {
      serviceClient = new ServiceClient(serviceEndpoint, username, password);
    } catch (ServiceException e) {
      throw new ResourceInitializationException(e);
    }
  }

  @Override
  public boolean hasNext() throws AnalysisEngineProcessException {
    return !serviceExecuted;
  }

  @Override
  public AbstractCas next() throws AnalysisEngineProcessException {
    Data input = new Data();
    input.setDiscriminator(CAS_DISCRIMINATOR);
    input.setPayload(serializeCas());
    Data output = serviceClient.execute(input);
    JCas outputCas = deserializeCas(output.getPayload());
    serviceExecuted = true;
    return outputCas;
  }

  @Override
  public void process(JCas aJCas) throws AnalysisEngineProcessException {
    cas = aJCas;
    serviceExecuted = false;
  }

  JCas deserializeCas(String payload) throws AnalysisEngineProcessException {
    JCas outputCas = getEmptyJCas();
    InputStream inputStream = new ByteArrayInputStream(payload.getBytes(
            CHARSET));
    try {
      XmiCasDeserializer.deserialize(inputStream, outputCas.getCas(), !mFailOnUnknownType);
    } catch (Exception e) {
      throw new AnalysisEngineProcessException(e);
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return outputCas;
  }

  String serializeCas() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    XmiCasSerializer serializer = new XmiCasSerializer(cas.getTypeSystem());
    try {
      serializer.serialize(cas.getCas(), baos);
    } catch (SAXException e) {
      e.printStackTrace();
    }
    String serCasString = null;
    try {
      serCasString = baos.toString(CHARSET.name());
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return serCasString;
  }

}
