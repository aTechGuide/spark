package in.kamranali;

import java.util.Arrays;

public class practice {

  public static void main(String[] args) {

    int arr[] = {1,2,3,4,5};
    System.out.println(Arrays.toString(arr));

    System.out.println(Arrays.toString(Arrays.copyOfRange(arr, 0, 2)));
  }

}
