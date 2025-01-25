package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializers._
import org.apache.flinkx.api.typeinfo.ProductTypeInformation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class GenericCaseClassScala3Test extends AnyFlatSpec with should.Matchers {

  import GenericCaseClassScala3Test._

  "Both TypeInformation of Animal Basket" should "have their respective TypeInformation of Animal" in {
    typeInformationOfAnimalBasketShouldHaveATypeInformationOfAnimal[Cat](classOf[Cat])
    // This second call is failing: TypeInformation of DogBasket hold a wrong TypeInformation of Cat
    typeInformationOfAnimalBasketShouldHaveATypeInformationOfAnimal[Dog](classOf[Dog])
  }

  def typeInformationOfAnimalBasketShouldHaveATypeInformationOfAnimal[A <: Animal: TypeTag: TypeInformation](
      aClass: Class[A]
  ): Unit = {
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Cat[] => OK
    val catInfo: TypeInformation[Cat]               = implicitly[TypeInformation[Cat]]
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Dog[] => OK
    val dogInfo: TypeInformation[Dog]               = implicitly[TypeInformation[Dog]]
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Cat[] or Dog[] => OK
    val aInfo: TypeInformation[A]                   = implicitly[TypeInformation[A]]
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Basket[org.apache.flinkx.api.GenericCaseClassTest.Cat[]] => OK
    val catBasketInfo: TypeInformation[Basket[Cat]] = implicitly[TypeInformation[Basket[Cat]]]
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Basket[org.apache.flinkx.api.GenericCaseClassTest.Dog[]] => OK
    val dogBasketInfo: TypeInformation[Basket[Dog]] = implicitly[TypeInformation[Basket[Dog]]]
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Basket[org.apache.flinkx.api.GenericCaseClassTest.typeInformationOfAnimalBasketShouldHaveATypeInformationOfAnimal.A[]] => issue
    val aBasketInfo: TypeInformation[Basket[A]]     = implicitly[TypeInformation[Basket[A]]]

    if (classOf[Cat].isAssignableFrom(aClass)) {
      aInfo should be theSameInstanceAs catInfo
      // aBasketInfo should be theSameInstanceAs catBasketInfo // Fails => cache miss
    }
    if (classOf[Dog].isAssignableFrom(aClass)) {
      aInfo should be theSameInstanceAs dogInfo
      // aBasketInfo should be theSameInstanceAs dogBasketInfo // Fails => cache miss
    }
    catBasketInfo.asInstanceOf[ProductTypeInformation[A]].getFieldTypes()(0) should be theSameInstanceAs catInfo
    dogBasketInfo.asInstanceOf[ProductTypeInformation[A]].getFieldTypes()(0) should be theSameInstanceAs dogInfo

    // This check is failing on second call: TypeInformation of DogBasket hold a wrong TypeInformation of Cat
    aBasketInfo.asInstanceOf[ProductTypeInformation[A]].getFieldTypes()(0) should be theSameInstanceAs aInfo
  }

}

object GenericCaseClassScala3Test {

  sealed trait Animal extends Product {
    def name: String
  }

  case class Cat(name: String) extends Animal
  case class Dog(name: String) extends Animal

  case class Basket[A <: Animal](animal: A)

}
