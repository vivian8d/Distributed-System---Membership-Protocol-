package cs425.mp3.Model;

import cs425.mp3.Utils;
import org.datavec.image.loader.NativeImageLoader;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.ImagePreProcessingScaler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;


public class Inference {

    public Inference() {}

    // load the model
    public MultiLayerNetwork getResnetModel()
            throws IOException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        MultiLayerNetwork model = KerasModelImport.importKerasSequentialModelAndWeights("Model/Resnet50.hdf5", false);
        return model;
    }

    public MultiLayerNetwork getVGGModel()
            throws IOException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        MultiLayerNetwork model = KerasModelImport.importKerasSequentialModelAndWeights("Model/VGG16.hdf5");

        return model;
    }


    public String inference(MultiLayerNetwork model, String filePath, String model_name) throws IOException {
        System.out.println(filePath);
        int height = 0;
        int width = 0;
        int channels = 3;
        System.out.println("[Model_name]: " + model_name);
        if (model_name.equals(Utils.Model1)) {
            height = 2048;
            width = 2048;
        }
        else if  (model_name.equals(Utils.Model2)) {
            height = 512;
            width = 512;
        }
        File image_file = new File(filePath);
        NativeImageLoader loader = new NativeImageLoader(height, width, channels);
        INDArray image = loader.asMatrix(image_file);

        DataNormalization scalar = new ImagePreProcessingScaler(0, 1);
        scalar.transform(image);

        INDArray new_image = image.reshape(1, channels, height, width);
//        System.out.println(Arrays.toString(new_image.shape()));

        String output = String.valueOf(model.output(new_image));
//        System.out.println(output);

        String input_file = filePath.split("\\.")[1];
        File tmpInf = File.createTempFile(model_name + "_output_" , input_file);
        writeOutput(output, tmpInf);
        return tmpInf.getPath();

    }


    public void writeOutput(String output, File tmpInf){
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(tmpInf.getPath(), false));
            writer.write(output);
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}