package org.endeavourhealth.informationmanager.common.transform;


import java.lang.reflect.Method;
import java.math.BigInteger;



/**A class which enables creation of a new Snomed identifier  to provide a check sum using verhoeff method, used by Snomed
 *
 */
public class SnomedConcept {
    private BigInteger concept;
    private String namespace;

    /**
     * Constructor for the concept creator which creates the concept on construction
     * @param leadingNumber the integer you wish to use to generate the Snomed id from
     *<p>Be very cautious not to use a leading number used elsewhere in this namespace</p>
     * @param ns the snomed namespace number, assumes this is an extension and thus long form id
     */
    public SnomedConcept(Integer leadingNumber, String ns) {
        namespace= ns;
        String rootConcept = leadingNumber.toString() + ns + "10";
        String appendChk = VerhoeffCheck.getCheckDigit(rootConcept).toString();
        concept = new BigInteger(rootConcept + appendChk);
    }

    /**
     * gets the Snomed concept id created when the object was instantiated
     * @return the long big integer that is the ID
     */
    public BigInteger getConcept() {
        return concept;
    }


    /**
     * Useful utility to create or validate Verhoeff Checksum integers
     */
    public static class VerhoeffCheck {
        /**
         * Creates the checksum digit from a string input
         * @param numStr  the string (which may be a number or a string) which requires the checksum
         * @return the checksum number 0-9
         */
        public static Integer getCheckDigit(String numStr) {
            //Dihedral array
            Integer di[][] = {
                    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                    {1, 2, 3, 4, 0, 6, 7, 8, 9, 5},
                    {2, 3, 4, 0, 1, 7, 8, 9, 5, 6},
                    {3, 4, 0, 1, 2, 8, 9, 5, 6, 7},
                    {4, 0, 1, 2, 3, 9, 5, 6, 7, 8},
                    {5, 9, 8, 7, 6, 0, 4, 3, 2, 1},
                    {6, 5, 9, 8, 7, 1, 0, 4, 3, 2},
                    {7, 6, 5, 9, 8, 2, 1, 0, 4, 3},
                    {8, 7, 6, 5, 9, 3, 2, 1, 0, 4},
                    {9, 8, 7, 6, 5, 4, 3, 2, 1, 0}};

            //Functional array
            int f0[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
            int f1[] = {1, 5, 7, 6, 2, 8, 3, 0, 9, 4};
            int[][] fnf = new int[8][10];
            for (int i = 0; i < 10; i++) {
                fnf[0][i] = f0[i];
                fnf[1][i] = f1[i];
            }
            for (int i = 2; i < 8; i++)
                for (int j = 0; j < 10; j++)
                    fnf[i][j] = fnf[i - 1][fnf[1][j]];

            //Inverse d5 array
            int[] inv = {0, 4, 3, 2, 1, 5, 6, 7, 8, 9};
            int check = 0;
            for (int i = numStr.length(); i >= 1; i--) {
                int j = Character.digit(numStr.charAt(i - 1), 10);
                check = di[check][fnf[(numStr.length() - i + 1) % 8][j]];
            }
            return inv[check];

        }
    }
}
