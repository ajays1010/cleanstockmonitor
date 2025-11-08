import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
// --- FIXED: Using a more stable, version-pinned import for firebase-admin ---
import admin from "https://esm.sh/v135/firebase-admin@11.9.0/es2022/firebase-admin.mjs";

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Initialize Firebase Admin SDK only once
try {
    const serviceAccount = JSON.parse(Deno.env.get("FIREBASE_SERVICE_ACCOUNT_KEY"));
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
    console.log("Firebase Admin SDK initialized successfully.");
} catch (e) {
    console.error("Firebase Admin SDK initialization error:", e.message);
}


serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    const { token } = await req.json();
    const decodedToken = await admin.auth().verifyIdToken(token);
    const { uid, phone_number } = decodedToken;

    // Create a Supabase client with the service_role key to bypass RLS
    const supabaseAdmin = createClient(
      Deno.env.get("SUPABASE_URL"),
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")
    );

    // Find user in profiles table by firebase_uid
    let { data: profile, error: profileError } = await supabaseAdmin
      .from("profiles")
      .select("id, email")
      .eq("firebase_uid", uid)
      .single();

    if (profileError && profileError.code !== "PGRST116") { // PGRST116 = not found
      throw profileError;
    }

    let supabaseUserId = profile?.id;
    let userEmail = profile?.email;

    // If no profile, find or create user in auth.users
    if (!profile) {
      const { data: newUser, error: authError } = await supabaseAdmin.auth.admin.createUser({
        phone: phone_number,
        email: `${phone_number}@yourapp.com`, // Placeholder email
        phone_confirm: true,
      });

      if (authError) throw authError;

      supabaseUserId = newUser.user.id;
      userEmail = newUser.user.email;

      // Now link this new user in the profiles table
      const { error: newProfileError } = await supabaseAdmin
        .from("profiles")
        .update({ firebase_uid: uid })
        .eq("id", supabaseUserId);

      if (newProfileError) throw newProfileError;
    }

    // Generate a custom session for the user
    const { data: sessionData, error: sessionError } = await supabaseAdmin.auth.admin.generateLink({
        type: 'magiclink',
        email: userEmail,
    });

    if (sessionError) throw sessionError;

    return new Response(JSON.stringify(sessionData.properties), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
      status: 200,
    });

  } catch (error) {
    return new Response(JSON.stringify({ error: error.message }), {
      headers: { ...corsHeaders, "Content-Type": "application/json" },
      status: 400,
    });
  }
});
