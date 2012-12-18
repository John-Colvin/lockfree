// Lock free Write Rarely Read Many associative array;
// Inspired by Andrei Alexandrescu's article "Lock free
// data structures"

//import core.atomic;
import std.stdio;
import std.exception;
import std.concurrency;
import core.thread;
import core.time;
import std.conv;
import std.random;

//could create a template to write all the cas code.

/**A lock free Associative array. Expensive to write to, cheap to read.
 * Currently any writing operation makes an entire copy of the array.
 * This could be improved with more information about how AAs are stored.
 */
shared struct LFAA (K, V) {
	private:
	
	alias V[K] T;
	alias shared(T) ST;
	T aa;

	public:
	
	//duplicates the data to ensure no external references can mess with it.
	this(U)(in U init) 
//		if(is(U == T) || is(U == ST))
	{
		aa = cast(ST)(init.dup);
	}

	auto opIndex(in K key) {
		return aa[key];
	}

	void opIndexAssign(in V value, in K key) {
//		writeln("writing ",value, " ", key);
		ST aaNew;
		T aaOld;
		do {
			aaOld = cast(T) aa;
			aaNew = cast(ST) aaOld.dup;
			aaNew[key] = value;
		} while(!cas(&aa, aaOld, aaNew));
	}
	
	@property auto length() {
		return aa.length;
	}
	
	/* *** */
	//can people write to these?
	@property auto keys() {
		return aa.keys;
	}
	
	@property auto values() {
		return aa.values;
	}
	/* *** */
	
	//In keeping with normal rehash. See below
	@property auto rehash() {
		ST aaNew;
		T aaOld;
		do {
			aaOld = cast(T) aa;
			aaNew = cast(ST) aaOld.dup;
			aaNew.rehash; //not a fan of this. It's a property with side effects...
		} while(!cas(&aa, aaOld, aaNew));
		return this;   //is this safe???
	}
	
	@property auto dup() {
		T aaOld = cast(T) aa;	//making a copy of the array pointers as I don't
								//trust dup to not need it.
		return LFAA!(K, V)(aaOld.dup);
	}
	
//	@property auto byKey() {}
//	@property auto byValue() {}    //not sure about these yet

	@property auto get(in K key, lazy V defaultValue) {
		return aa.get(key, defaultValue);		
	}
	
	@property void remove(in K key) {
		ST aaNew;
		T aaOld;
		do {
			aaOld = cast(T) aa;
			aaNew = cast(ST) aaOld.dup;
			aaNew.remove(key);
		} while(!cas(&aa, aaOld, aaNew));
	}
}

void main() {}

unittest {
	import std.stdio;
	
	string[] alphabet = ["a","b","c","d","e","f","g","h","i","j",
	"k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"];

	void torture_aa(shared(LFAA!(string, int))* array, Tid parent) {
		foreach(int i, letter; alphabet) {
			(*array)[letter] = i;
			if(uniform(0f,1f) > 0.95) {
				(*array).rehash();
			}
		}
	}
	
	void wait_for_threads(T)(T[] threads, Duration delay = dur!("msecs")(50)) {
		bool running = true;
		while(running) {
			Thread.sleep(delay);
			running = false;
			foreach(ref thread; threads) {
				if(thread.isRunning) {
					running = true;
					break;
				}
			}
		}
		foreach(ref thread; threads)
			enforce(!(thread.isRunning));
	}
	
	const test_length = 750;   //hangs above this...
	
	int[string] arr = ["foo":1, "bar":2];
	shared auto test = LFAA!(string, int)(arr);

	Thread[] threads = new Thread[test_length];
	
	void exec() {
		torture_aa(&test, thisTid);
	}
	
	//create test threads
	foreach(i;0..test_length) {
		threads[i] = new Thread(&exec);
	}
	
	writeln("torturing:");
	//run test threads
	foreach(i;0..test_length) {
		threads[i].start();
	}
	
	wait_for_threads(threads);

	//check for consistency
	writeln("checking:");
	foreach(int i, letter; alphabet) {
		enforce(test[letter] == i);
	}
	writeln("PASSED!");
}

/**A lock free Array. Expensive to write to, cheap to read.
 */
shared struct LFArr (V) {
	private:
	
	alias V[] T;
	alias shared(T) ST;
	T arr;
	
	public:

	//duplicates the data to ensure no external references can mess with it.
	this(U)(in U init) 
//		if(is(U == T) || is(U == ST))
	{
		arr = cast(ST)(init.dup);
	}

	auto opIndex(in size_t index) {
		return arr[index];
	}

	void opIndexAssign(in V value, in size_t index) {
		ST arrNew;
		T arrOld;
		do {
			arrOld = cast(T) arr;
			arrNew = cast(ST) arrOld.dup;
			arrNew[index] = value;
		} while(!cas(&arr, arrOld, arrNew));
	}
	
	@property auto length() {
		return arr.length;
	}
	
	@property auto length(in size_t newLength) {
		//do something....
	}
	
	//no .ptr ???
	
	@property auto dup() {
		T arrOld = cast(T) arr;	//making a copy of the array pointers as I don't
								//trust dup to not need it.
		return LFArr!T(arrOld.dup);
	}
	
	@property auto idup() {
		return cast(immutable)this.dup;
	}

	@property auto reverse() {
		T arrOld = cast(T) arr;	//making a copy of the array pointers as I don't
								//trust reverse to not need it.
		return LFArr!T(arrOld.reverse);
	}
	
	//does anyone use this? what's it like compared to the sort in std.algorithm?
	@property auto sort() {
		T arrOld = cast(T) arr;	//making a copy of the array pointers as I don't
								//trust sort to not need it.
		return LFArr!T(arrOld.sort);
	}
}
/*
unittest {
	import std.stdio;
	
	double[] data = new double[100];

	void torture_arr(shared(LFArr!double)* array, Tid parent) {
		foreach(int i, d; data) {
			(*array)[letter] = i;
			if(uniform(0f,1f) > 0.95) {
				(*array).rehash();
			}
		}
	}
	
	void wait_for_threads(T)(T[] threads, Duration delay = dur!("msecs")(50)) {
		bool running = true;
		while(running) {
			Thread.sleep(delay);
			running = false;
			foreach(ref thread; threads) {
				if(thread.isRunning) {
					running = true;
					break;
				}
			}
		}
		foreach(ref thread; threads)
			enforce(!(thread.isRunning));
	}
	
	const test_length = 750;   //hangs above this...
	
	int[string] arr = ["foo":1, "bar":2];
	shared auto test = LFAA!(string, int)(arr);

	Thread[] threads = new Thread[test_length];
	
	void exec() {
		torture_aa(&test, thisTid);
	}
	
	//create test threads
	foreach(i;0..test_length) {
		threads[i] = new Thread(&exec);
	}
	
	writeln("torturing:");
	//run test threads
	foreach(i;0..test_length) {
		threads[i].start();
	}
	
	wait_for_threads(threads);

	//check for consistency
	writeln("checking:");
	foreach(int i, letter; alphabet) {
		enforce(test[letter] == i);
	}
	writeln("PASSED!");
}
*/
    bool cas(T,V1,V2)( shared(T)* here, const V1 ifThis, const V2 writeThis ) nothrow
        if( !is(T == class) && !is(T U : U*) &&  __traits( compiles, { *here = writeThis; } ) )
    {
        return casImpl(here, ifThis, writeThis);
    }

    bool cas(T,V1,V2)( shared(T)* here, const shared(V1) ifThis, shared(V2) writeThis ) nothrow
        if( is(T == class) && __traits( compiles, { *here = writeThis; } ) )
    {
        return casImpl(here, ifThis, writeThis);
    }

    bool cas(T,V1,V2)( shared(T)* here, const shared(V1)* ifThis, shared(V2)* writeThis ) nothrow
        if( is(T U : U*) && __traits( compiles, { *here = writeThis; } ) )
    {
        return casImpl(here, ifThis, writeThis);
    }

	bool cas(T,V1,V2)( shared(T)* here, V1 ifThis, V2 writeThis ) nothrow
        if( (is(T U : U[]) || __traits(isAssociativeArray, T)) && __traits( compiles, { *here = writeThis; } ) )
	{
		return casImpl(here, ifThis, writeThis);
	}

    private bool casImpl(T,V1,V2)( shared(T)* here, V1 ifThis, V2 writeThis ) nothrow
    in
    {
        // NOTE: 32 bit x86 systems support 8 byte CAS, which only requires
        //       4 byte alignment, so use size_t as the align type here.
        static if( T.sizeof > size_t.sizeof )
            assert( atomicValueIsProperlyAligned!(size_t)( cast(size_t) here ) );
        else
            assert( atomicValueIsProperlyAligned!(T)( cast(size_t) here ) );
    }
    body
    {
        static if( T.sizeof == byte.sizeof )
        {
            //////////////////////////////////////////////////////////////////
            // 1 Byte CAS
            //////////////////////////////////////////////////////////////////

            asm
            {
                mov DL, writeThis;
                mov AL, ifThis;
                mov RCX, here;
                lock; // lock always needed to make this op atomic
                cmpxchg [RCX], DL;
                setz AL;
            }
        }
        else static if( T.sizeof == short.sizeof )
        {
            //////////////////////////////////////////////////////////////////
            // 2 Byte CAS
            //////////////////////////////////////////////////////////////////

            asm
            {
                mov DX, writeThis;
                mov AX, ifThis;
                mov RCX, here;
                lock; // lock always needed to make this op atomic
                cmpxchg [RCX], DX;
                setz AL;
            }
        }
        else static if( T.sizeof == int.sizeof )
        {
            //////////////////////////////////////////////////////////////////
            // 4 Byte CAS
            //////////////////////////////////////////////////////////////////

            asm
            {
                mov EDX, writeThis;
                mov EAX, ifThis;
                mov RCX, here;
                lock; // lock always needed to make this op atomic
                cmpxchg [RCX], EDX;
                setz AL;
            }
        }
        else static if( T.sizeof == long.sizeof )
        {
            //////////////////////////////////////////////////////////////////
            // 8 Byte CAS on a 64-Bit Processor
            //////////////////////////////////////////////////////////////////

            asm
            {
                mov RDX, writeThis;
                mov RAX, ifThis;
                mov RCX, here;
                lock; // lock always needed to make this op atomic
                cmpxchg [RCX], RDX;
                setz AL;
            }
        }
        else
        {
            static assert( false, "Invalid template type specified." );
        }
    }


    private bool atomicValueIsProperlyAligned(T)( size_t addr ) pure nothrow
    {
        return addr % T.sizeof == 0;
    }

