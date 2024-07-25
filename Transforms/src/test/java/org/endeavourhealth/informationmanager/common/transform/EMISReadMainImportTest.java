package org.endeavourhealth.informationmanager.common.transform;

import org.endeavourhealth.informationmanager.transforms.sources.EMISImport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import static org.junit.jupiter.api.Assertions.*;

@RunWith(JUnitPlatform.class)
class EMISReadMainImportTest {

  private EMISImport transform;

  @BeforeEach
  void setup() {
    transform = new EMISImport();
  }

  @Test
  void getEmisCode_11() {
    String expected = "H33z-1";
    String actual = transform.getEmisCode("H33z.", "11HMW");

    assertEquals(expected, actual);
  }


  @Test
  void getEmisCode_00() {
    String expected = "H33z";
    String actual = transform.getEmisCode("H33z.", "00HMW");

    assertEquals(expected, actual);
  }

  @Test
  void getEmisCode_none_11() {
    String expected = "H333z-1";
    String actual = transform.getEmisCode("H333z", "11HMW");

    assertEquals(expected, actual);
  }

  @Test
  void getEmisCode_none_00() {
    String expected = "H333z";
    String actual = transform.getEmisCode("H333z", "00HMW");

    assertEquals(expected, actual);
  }

  @Test
  void getEmisCode_none_22() {
    String expected = "Eu453-22";
    String actual = transform.getEmisCode("Eu453", "22Fta");

    assertEquals(expected, actual);
  }

  @Test
  void getEmisCode_() {
    String expected = "S840-2";
    String actual = transform.getEmisCode("S840.", "12MOP");

    assertEquals(expected, actual);
  }

  @Test
  void getNameSpace() {
    String expected = "1000001";
    String actual = transform.getNameSpace("654011000001109");
    assertEquals(expected, actual);
  }
}
