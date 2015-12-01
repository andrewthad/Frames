{-# LANGUAGE BangPatterns,
             DataKinds,
             EmptyCase,
             FlexibleInstances,
             ScopedTypeVariables,
             TupleSections,
             RankNTypes,
             ConstraintKinds,
             TypeFamilies,
             TypeOperators,
             FlexibleContexts,
             UndecidableInstances #-}
-- | Efficient in-memory (in-core) storage of tabular data. This is 
--   a modification of the InCore file that attempts to do recursion
--   at the value level instead of the type level.
module Frames.InCore2 where
import Control.Applicative
import Control.Monad.Primitive
import Control.Monad.ST (runST)
import Data.Proxy
import Data.Text (Text)
import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vinyl as V
import Data.Vinyl (Rec(..),rtraverse)
import Data.Vinyl.Functor (Identity(..))
import Frames.Col
import Frames.Frame
import Frames.Rec
-- import Frames.RecF
import GHC.Prim (RealWorld)
import qualified Pipes as P
import qualified Pipes.Prelude as P
import Frames.TypeLevel

newtype VectorVal t  = VectorVal { getVectorVal :: VectorFor t t }
newtype VectorMVal s t = VectorMVal { getVectorMVal :: VG.Mutable (VectorFor t) s t }

class ( VGM.MVector (VG.Mutable (VectorFor t)) t
      , VG.Vector (VectorFor t) t
      ) 
  => VectorForClass t where
    type VectorFor t :: * -> *

initialCapacity :: Int
initialCapacity = 128

rtraverseConstrained
  :: (Applicative h, LAll c rs)
  => Proxy c 
  -> (forall x. c x => f x -> h (g x))
  -> Rec f rs
  -> h (Rec g rs)
rtraverseConstrained _ _ RNil      = pure RNil
rtraverseConstrained c f (x :& xs) = (:&) <$> f x <*> rtraverseConstrained c f xs

allocRecIO :: LAll VectorForClass rs
           => Rec g rs -> IO (Rec (VectorMVal RealWorld) rs)
allocRecIO = rtraverseConstrained (Proxy :: Proxy VectorForClass)
  (const (fmap VectorMVal (VGM.new initialCapacity)))

freezeRecIO :: LAll VectorForClass rs
            => Rec (VectorMVal RealWorld) rs 
            -> IO (Rec VectorVal rs)
freezeRecIO = rtraverseConstrained (Proxy :: Proxy VectorForClass)
  (fmap VectorVal . VG.unsafeFreeze . getVectorMVal)

