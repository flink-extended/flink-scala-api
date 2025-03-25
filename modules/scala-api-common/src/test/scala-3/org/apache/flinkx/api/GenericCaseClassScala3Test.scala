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
    typeInformationOfAnimalBasketShouldHaveATypeInformationOfAnimal[Dog](classOf[Dog])
  }

  def typeInformationOfAnimalBasketShouldHaveATypeInformationOfAnimal[A <: Animal: TypeTag: TypeInformation](
      aClass: Class[A]
  ): Unit = {
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Cat => OK
    val catInfo: TypeInformation[Cat] = deriveTypeInformation
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Dog => OK
    val dogInfo: TypeInformation[Dog] = deriveTypeInformation
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Cat or Dog => OK
    val aInfo: TypeInformation[A] = implicitly[TypeInformation[A]]
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Basket[org.apache.flinkx.api.GenericCaseClassTest.Cat] => OK
    val catBasketInfo: TypeInformation[Basket[Cat]] = deriveTypeInformation
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Basket[org.apache.flinkx.api.GenericCaseClassTest.Dog] => OK
    val dogBasketInfo: TypeInformation[Basket[Dog]] = deriveTypeInformation
    // cacheKey=org.apache.flinkx.api.GenericCaseClassTest.Basket[A] => Basket[A] is not cachable
    val aBasketInfo: TypeInformation[Basket[A]] = deriveTypeInformation

    if (classOf[Cat].isAssignableFrom(aClass)) {
      aInfo should be theSameInstanceAs catInfo
      aBasketInfo shouldNot be theSameInstanceAs catBasketInfo // Basket[A] is not cachable
    }
    if (classOf[Dog].isAssignableFrom(aClass)) {
      aInfo should be theSameInstanceAs dogInfo
      aBasketInfo shouldNot be theSameInstanceAs dogBasketInfo // Basket[A] is not cachable
    }
    catBasketInfo.asInstanceOf[ProductTypeInformation[A]].getFieldTypes()(0) should be theSameInstanceAs catInfo
    dogBasketInfo.asInstanceOf[ProductTypeInformation[A]].getFieldTypes()(0) should be theSameInstanceAs dogInfo
    // Type info of Basket[A] is not cached, but it holds a type info of the good type (Cat or Dog) found in the cache
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
