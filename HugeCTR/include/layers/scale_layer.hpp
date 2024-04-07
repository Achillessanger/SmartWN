/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
 *
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
#pragma once

#include <layer.hpp>

namespace HugeCTR {

/**
 * Relu activation function as a derived class of Layer
 */
template <typename T>
class ScaleLayer : public Layer {
  /*
   * stores the references to the input tensors of this layer.
   */
  std::vector<core23::Tensor> in_tensors_;
  /*
   * stores the references to the output tensors of this layer.
   */
  std::vector<core23::Tensor> out_tensors_;

 public:
  /**
   * Ctor of ReluLayer.
   * @param in_tensor the input tensor
   * @param out_tensor the output tensor which has the same dim with in_tensor
   * @param device_id the id of GPU where this layer belongs
   */
  ScaleLayer(const core23::Tensor& in_tensor, core23::Tensor& out_tensor, int axis, int factor,
             const std::shared_ptr<GPUResource>& gpu_resource);

  /**
   * A method of implementing the forward pass of Relu
   * @param stream CUDA stream where the forward propagation is executed
   */
  void fprop(bool is_train) override;
  /**
   * A method of implementing the backward pass of Relu
   * @param stream CUDA stream where the backward propagation is executed
   */
  void bprop() override;

  int axis_;
  int factor_;
};

}  // namespace HugeCTR
