import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Stream;


public class Main
{
    public static void main(String [] args)
    {
//
        int[] A = {4, 2, 2, 5, 1, 5, 8}; // [1, 2, 2, 4, 5, 5, 8]
        System.out.println(solution(A));

    }

    public static int solution(int[] A) {
        Map<Integer, Integer> map = new TreeMap<>();
        for(int i = 0; i < A.length; i++) {
           map.put(A[i], i);
        }

        float minAvg = Float.MAX_VALUE;
        int minPointer = 0;

        for(int i = 0; i < map.entrySet().size(); i++) {
            Map.Entry<Integer, Integer> entry = (Map.Entry<Integer, Integer>) map.entrySet().toArray()[i];
            int value = entry.getKey();
            int index = entry.getValue();

            List<Integer> indices = new ArrayList<>();
            if(index == 0) {
                indices.add(1);
            } else if(index == A.length - 1) {
                indices.add(A.length - 2);
            } else {
                indices.add(index - 1);
                indices.add(index + 1);
            }
        }

//        for(Map.Entry<Integer, Integer> entry : map.entrySet()) {
//            float avg = entry.getKey();
//            int index = entry.getValue(),
//                pointer = index,
//                count  = 1;
//            if(avg > minAvg)
//                return minPointer;
//            while(++pointer < A.length) {
//                avg = (avg * count + A[pointer]) / (count + 1);
//                count++;
//                if(avg < minAvg) {
//                    minAvg = avg;
//                    minPointer = index;
//                }
//            }
//
//        }
        return minPointer;
    }

//    public static void solution(int N) {
//        // write your code in Java SE 8
//        if (N < 0)
//            System.out.println(""); //if N is negative, print nothing
//        for (int i = 1; i <= N; i++) {
//            if(i % 3 == 0 || i % 5 == 0 || i % 7 == 0) {
//                String str = "";
//                if(i % 3 == 0)
//                    str += "Fizz";
//                if(i % 5 == 0)
//                    str += "Buzz";
//                if(i % 7 == 0)
//                    str += "Woof";
//                System.out.println(str);
//            } else {
//                System.out.println(i);
//            }
//        }
//    }

}


//class Solution {
//    public int solution(int A, int B) {
//        // write your code in Java SE 8
//        int count = 0;
//        for (int i = A; i <= B; i++) {
//            ArrayList<Integer> digits = new ArrayList<>();
//            int num = i;
//            while(num != 0) {
//                int digit = num % 10;
//                num = num / 10;
//                digits.add(digit);
//            }
//            Stream<Integer> streamedDigits = digits.stream();
//            if(digits.size() != 0){
//                float average = (float) streamedDigits.reduce(0, (a, b) -> a + b) / streamedDigits.count();
//                if (average > 7){
//                    count++;
//                }
//            }
//
//        }
//        return count;
//    }
//}
//
//
//class SolutionIter implements Iterable<Integer> {
//
//    private Reader reader;
//    private String chars = "";
//
//    public SolutionIter(Reader inp) {
//        this.reader = inp;
//        try {
//            StringBuilder sb = new StringBuilder();
//            int data = reader.read();
//            while(data != -1){
//                char dataChar = (char) data;
//                sb.append(dataChar);
//                data = reader.read();
//            }
//            this.chars = sb.toString();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public Iterator<Integer> iterator() {
//
//        String[] lines = this.chars.split("\\\\r?\\\\n");
//        ArrayList<Integer> integerList = new ArrayList<>();
//
//        for (String line : lines) {
//            try {
//                int integer = Integer.parseInt(line.trim());
//                if (integer < 1000000000 && integer > -1000000000) {
//                    integerList.add(integer);
//                }
//            } catch (NumberFormatException ignored) {
//            }
//        }
//
//
//        Iterator<Integer> it = new Iterator<Integer>() {
//
//            private int currentIndex = 0;
//
//            @Override
//            public boolean hasNext() {
//                return currentIndex < integerList.size() && integerList.get(currentIndex) != null;
//            }
//
//            @Override
//            public Integer next() {
//                Integer integer = integerList.get(currentIndex);
//                currentIndex++;
//                return integer;
//            }
//
//            @Override
//            public void remove() {
//                throw new UnsupportedOperationException();
//            }
//        };
//        return it;
//    }
//}

//public class CopyBit
//{
//    public static int copyBit(int source, int destination, int position)
//    {
//
//        int bit = (source >> position) & 1;
//
//        if(bit == 1)
//           return destination | (1 << position);
//        else
//           return destination & ~(1 << position);
//    }
//
//    public static void main(String[] args) {
//        System.out.println(CopyBit.copyBit(7, 12, 3));
//    }
//}